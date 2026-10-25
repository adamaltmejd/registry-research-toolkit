"""Content-level diff for two reg_meta SQLite DBs.

This is the acceptance gate for the Model A migration's adapter refactor
(Model A migration stage A4.1): A4.1 moves SCB ingest out of
`reg_meta_build.db`'s `_import_*` functions into an IR-emitting adapter +
a provider-blind materializer, and MUST produce a universal DB whose
*content* is identical to the pre-A4 baseline.

**Raw file bytes are not a valid check.** Two SQLite files with identical
logical content differ byte-wise: page layout, freelist state, vacuum
generation, and (for FTS5) inverted-index segment layout all depend on
insert order and history, not on the rows you can query back. So this tool
compares *content*, order-independently.

What it does (see the four numbered requirements in the A4.1 brief):

1. **Schema compare** — same set of tables, and per table the same column
   names/types/order/NOT NULL/default/PK plus the same normalized CREATE
   statement, and the same indexes. Comparing the CREATE text catches what
   `PRAGMA table_info` cannot: table-level UNIQUE/CHECK/FK constraints (so a
   `UNIQUE(a)` → `UNIQUE(b)` change, which leaves the `sqlite_autoindex_*` name
   identical, is still flagged) and FTS5 options (tokenize, content,
   content_rowid). Tables/columns/indexes present in only one DB are reported.

2. **Per-table content compare, order- and BLOB-safe** — for every user
   content table, compare row COUNT and an order-independent *multiset*
   fingerprint of its natural-key projection (below). Each row is canonicalized to a type-tagged byte string
   (NULL-aware, BLOB-as-bytes, ints/floats/text given distinct encodings),
   hashed with BLAKE2b-128, and the per-row hashes are summed mod 2**128.
   Summation (not XOR) is deliberate: XOR cancels duplicate rows, so it
   could not tell `[R, R]` from `[]`; modular addition is multiplicity-
   sensitive, so duplicate-row and missing-row differences are detected.
   The pass is O(n) streaming — one cursor, fetched in batches, O(1) memory
   — so the 5.7M-row `value_set_member` table costs a few seconds and a few
   kB, not 330 MB of RAM.

3. **Ignore list** — only legitimately nondeterministic build metadata is
   excluded, and the default is as small as possible. The reg_meta schema
   has exactly one wall-clock field: `import_manifest`'s `import_date` row
   (an ISO timestamp). `schema_version` and every content column are
   compared; surrogate ids compare through their natural keys (below). `input_dir` (an absolute path) is *not* ignored by default —
   it is identical for a same-machine rebuild (the A4.1 case); a
   cross-machine comparison can add it explicitly. There is no other
   build/provenance table in the shipped DB (`build_manifest` is scaffolded
   but unpopulated and absent here). Pass `ignore={}` to compare everything.

4. **Actionable mismatch output** — a mismatch names the table, the count
   delta, and the first N differing rows in each direction (present-in-A,
   present-in-B), found via an ATTACH-based multiset-difference query that
   offloads its working set to SQLite's on-disk temp store. `DiffReport`
   carries the structured result; the CLI exits non-zero on any difference.

FTS5 handling: the four catalog `*_fts` tables store `fold_search`
projections of base tables that ARE compared (`validate_built_db` checks
each index against its source both ways, within one artifact; two artifacts
that differ only in the fold function therefore diff as identical).
`doc_fts` in the docs DB is still external-content (`content='doc'`).
The shadow tables (`*_fts_data/_idx/_docsize/_config/_content`) hold the
serialized index and stored text, whose bytes depend on insert order — comparing them would false-positive on
an emit-order change that left content identical. So FTS virtual + shadow
tables are included in the *schema* comparison (structure must match) but
excluded from the *content* comparison. `sqlite_*` internal tables
(`sqlite_sequence`, `sqlite_stat*`) are excluded throughout.

**Catalog ids compare by natural key.** The catalog's internal integer ids
(`variable_id`, `code_id`, `state_id`, ...) are dense and reassigned on every
build, so one added variable shifts the ids of every later one. When both DBs
are catalogs (they hold every table in `_IDS`), rows are compared on a
projection, never on raw ids, driven by an explicit map of the catalog's ids:

- `_IDS` names each table's own id column, left out of its rows, and the
  natural key that identifies its rows (the FQIDs, a code's ``(code, label)``,
  a state's key, ...);
- `_REFERENCES` names every column that holds one of those ids and the table
  that owns it. It compares as a tagged JSON array: ``["k", key]`` with the
  row's natural key, ``["d", id]`` for an id no row holds. A NULL stays NULL.
  A key is a JSON array of its columns (a NULL column is JSON null); a data
  value in it is a JSON scalar (a BLOB a ``{"blob": hex}`` object) and a
  reference in it nests its own tagged array, so a reference never equals
  data or another kind of reference.

Before the diff, each catalog is checked against the map (`_check_catalog`),
and any failure exits 2 with a named reason rather than a guess: a mapped
column missing; an INTEGER primary key or INTEGER foreign key the map does not
cover (so an id column added to the DDL without updating this map fails
closed); a NULL or non-integer id; a natural key the data does not hold unique.

So two catalog builds that differ only in id assignment compare identical. The
comparison uses the stored JSON text; reports decode it for display only
(`_display`), so a sample row shows ``variable_id = [["scb", "lisa"], "kon"]``,
not ``4711``. Any other pair of DBs (a docs DB, a test fixture) compares raw
rows, ids included.

Canonicalization is type-discriminated: SQLite storage class (NULL /
INTEGER / REAL / TEXT / BLOB) is part of the fingerprint, so an ID that
turned from an int into a float would be flagged. The reg_meta schema is
float-free (only INTEGER/TEXT/BLOB columns), so this never produces a
spurious diff in practice; it just makes the gate strict.

This is a standalone, read-only tool — it opens both DBs with `mode=ro`
URIs (the natural-key maps live in a private temp store) and works on any two
reg_meta DB files. It does not import or touch
the build pipeline.

CLI:  ``python -m reg_meta_build.dbdiff <db_a> <db_b> [-n N] [--no-default-ignore] [--json]``
Exit: 0 identical · 1 differs · 2 usage/IO error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import struct
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

# BLAKE2b digest size (bytes) and the modulus for the commutative row-sum.
_HASH_BYTES = 16
_FP_MODULUS = 1 << (_HASH_BYTES * 8)

# FTS5 shadow-table name suffixes. A table is an FTS shadow iff its name is
# `<virtual_table>_<suffix>` for one of these and a matching virtual table
# exists. (`content`/`docsize` only appear for some option combinations; the
# full set keeps detection robust across FTS5 configurations.)
_FTS_SHADOW_SUFFIXES = ("data", "idx", "docsize", "config", "content", "row")


@dataclass(frozen=True)
class TableIgnore:
    """Per-table exclusions for the content comparison.

    ``skip_where`` is a SQL boolean predicate identifying rows to exclude from
    BOTH DBs (applied as ``WHERE NOT (<skip_where>)``); it also lowers the
    compared row count, so an ignored row never shows up as a count delta.
    """

    skip_where: str | None = None


# Default ignore set — deliberately minimal (see requirement 3 in the module
# docstring). Only `import_manifest.import_date` is nondeterministic.
DEFAULT_IGNORE: dict[str, TableIgnore] = {
    "import_manifest": TableIgnore(skip_where="key = 'import_date'"),
}

# The catalog's surrogate ids (db.py DDL), dense and reassigned on every build.
# table -> (its id column, the natural key that identifies its rows). A key column
# that is itself a reference nests that row's key. None: nothing references the
# table, so its id is only left out of its rows. Each key is checked unique on the
# data before a diff (`_check_catalog`).
_IDS: dict[str, tuple[str, tuple[str, ...] | None]] = {
    "provider": ("provider_id", ("slug",)),
    # The FQIDs (crates/DESIGN.md → FQID grammar).
    "register": ("register_id", ("provider_id", "slug")),
    "register_variant": ("register_variant_id", ("register_id", "slug")),
    "variable": ("variable_id", ("register_id", "slug")),
    # SCB's register-version grain within a variant.
    "register_version": ("regver_id", ("register_variant_id", "registerversionnamn")),
    "classification": ("id", ("slug",)),
    "value_code": ("code_id", ("code", "label")),
    "value_set": ("value_set_id", ("member_hash",)),
    # The union of its two partial UNIQUE indexes, which split on period_scope.
    "variable_state": (
        "state_id",
        (
            "variable_id",
            "register_variant_id",
            "period_scope",
            "valid_from",
            "value_set_version_label",
        ),
    ),
    "browse_delivery": (
        "browse_delivery_id",
        ("scope", "variable_id", "register_variant_id", "delivery_column_name"),
    ),
    # Their UNIQUE indexes, which COALESCE the nullable column.
    "concept_group": ("group_id", ("kind", "register_id", "group_key")),
    "concept_group_variable": (
        "member_id",
        ("group_id", "variable_id", "delivery_column_name"),
    ),
    "data_warning_text": ("text_id", ("text",)),
    "holding_table": ("table_id", ("physical_id",)),
    "holding_column": ("column_id", ("table_id", "name")),
    "tag": ("tag_id", ("slug",)),
    "expanded_state": ("expanded_state_id", None),
    "source_relationship": ("relationship_id", None),
}

# Every column that holds one of those ids -> the table that owns it. A composite
# foreign key's id columns name the owner at the end of the chain
# (alias_window_classification.variable_id → variable_alias_window → variable);
# its TEXT columns are natural values and compare as they are. A primary key that
# is a reference (variable_search_text.variable_id) is compared by what it
# references, never left out.
_REFERENCES: dict[tuple[str, str], str] = {
    ("alias_window_classification", "classification_id"): "classification",
    ("alias_window_classification", "register_variant_id"): "register_variant",
    ("alias_window_classification", "variable_id"): "variable",
    ("browse_delivery", "register_variant_id"): "register_variant",
    ("browse_delivery", "variable_id"): "variable",
    ("classification", "supersedes_id"): "classification",
    ("classification_code", "classification_id"): "classification",
    ("classification_code", "code_id"): "value_code",
    ("classification_code", "parent_code_id"): "value_code",
    ("classification_conformance", "declared_classification_id"): "classification",
    ("classification_conformance", "state_id"): "variable_state",
    ("classification_conformance_code", "code_id"): "value_code",
    ("classification_conformance_code", "declared_classification_id"): (
        "classification"
    ),
    ("classification_conformance_code", "state_id"): "variable_state",
    ("code_variable_map", "code_id"): "value_code",
    ("code_variable_map", "variable_id"): "variable",
    ("concept_group", "register_id"): "register",
    ("concept_group_axis", "group_id"): "concept_group",
    ("concept_group_classification", "classification_id"): "classification",
    ("concept_group_classification", "group_id"): "concept_group",
    ("concept_group_variable", "group_id"): "concept_group",
    ("concept_group_variable", "variable_id"): "variable",
    ("concept_group_variable_facet", "member_id"): "concept_group_variable",
    ("data_warning", "detail_id"): "data_warning_text",
    ("data_warning", "register_id"): "register",
    ("data_warning", "register_variant_id"): "register_variant",
    ("data_warning", "summary_id"): "data_warning_text",
    ("data_warning", "variable_id"): "variable",
    ("delivery_window", "browse_delivery_id"): "browse_delivery",
    ("expanded_state", "register_variant_id"): "register_variant",
    ("expanded_state", "state_id"): "variable_state",
    ("expanded_state", "variable_id"): "variable",
    ("holding_column", "table_id"): "holding_table",
    ("holding_mapping", "column_id"): "holding_column",
    ("holding_mapping", "variable_id"): "variable",
    ("holding_mapping", "variant_id"): "register_variant",
    ("holding_key", "column_id"): "holding_column",
    ("holding_key", "variable_id"): "variable",
    ("holding_key", "variant_id"): "register_variant",
    ("holding_period", "table_id"): "holding_table",
    ("object_type", "regver_id"): "register_version",
    ("population", "regver_id"): "register_version",
    ("register", "provider_id"): "provider",
    ("register_variant", "register_id"): "register",
    ("register_version", "register_variant_id"): "register_variant",
    ("resolver_column", "register_variant_id"): "register_variant",
    ("resolver_column", "variable_id"): "variable",
    ("source_relationship", "owner_variable_id"): "variable",
    ("state_classification", "classification_id"): "classification",
    ("state_classification", "state_id"): "variable_state",
    ("tag_member", "register_id"): "register",
    ("tag_member", "tag_id"): "tag",
    ("tag_member", "variable_id"): "variable",
    ("value_set_member", "code_id"): "value_code",
    ("value_set_member", "value_set_id"): "value_set",
    ("variable", "register_id"): "register",
    ("variable", "source_register_id"): "register",
    ("variable_alias", "register_variant_id"): "register_variant",
    ("variable_alias", "variable_id"): "variable",
    ("variable_alias_window", "register_variant_id"): "register_variant",
    ("variable_alias_window", "value_set_id"): "value_set",
    ("variable_alias_window", "variable_id"): "variable",
    # Declared without a FOREIGN KEY clause: the derived search table.
    ("variable_search_text", "register_id"): "register",
    ("variable_search_text", "variable_id"): "variable",
    ("variable_state", "register_variant_id"): "register_variant",
    ("variable_state", "value_set_id"): "value_set",
    ("variable_state", "variable_id"): "variable",
    ("variable_state_lineage", "consumer_state_id"): "variable_state",
    ("variable_state_lineage", "source_state_id"): "variable_state",
    ("variable_state_lineage_warning", "consumer_state_id"): "variable_state",
}

# INTEGER primary keys and foreign keys that are not ids and compare as they are.
# Empty today: every one the catalog declares is an id above.
_COMPARE_RAW: frozenset[tuple[str, str]] = frozenset()


@dataclass(frozen=True)
class ColumnDiff:
    """Per-table schema difference."""

    table: str
    only_in_a: tuple[str, ...]
    only_in_b: tuple[str, ...]
    # (column, a_definition, b_definition) for columns present in both whose
    # type/order/notnull/default/pk differ.
    mismatched: tuple[tuple[str, str, str], ...]
    # Normalized CREATE statements when they differ (else both ""). Catches
    # what PRAGMA table_info can't see: table-level UNIQUE/CHECK/FK constraints
    # and FTS5 options (tokenize, content, content_rowid). May be set even when
    # the column lists match (a pure constraint/option change).
    create_sql_a: str = ""
    create_sql_b: str = ""

    @property
    def definition_differs(self) -> bool:
        return self.create_sql_a != self.create_sql_b

    @property
    def differs(self) -> bool:
        return bool(
            self.only_in_a
            or self.only_in_b
            or self.mismatched
            or self.definition_differs
        )


@dataclass(frozen=True)
class SampleRow:
    """A differing row from the multiset diff.

    ``net`` is (#copies in A) - (#copies in B) for this exact row tuple:
    positive means A has extra copies, negative means B does. ``values`` is
    aligned with ``DiffReport.table_columns[table]``.
    """

    net: int
    values: tuple[object, ...]


@dataclass(frozen=True)
class TableContentResult:
    """Content comparison result for one shared table."""

    table: str
    count_a: int
    count_b: int
    fingerprint_a: str  # hex, "" when skipped
    fingerprint_b: str
    columns: tuple[str, ...]  # columns compared; () when skipped
    # The compared columns that hold a tagged reference (JSON text, `_link_sql`).
    link_columns: tuple[str, ...] = ()
    sample_a_not_b: tuple[SampleRow, ...] = ()
    sample_b_not_a: tuple[SampleRow, ...] = ()
    skipped_reason: str | None = None
    note: str | None = None

    @property
    def identical(self) -> bool:
        if self.skipped_reason is not None:
            return False
        return self.count_a == self.count_b and self.fingerprint_a == self.fingerprint_b


@dataclass
class DiffReport:
    """Structured result of comparing two DBs.

    ``identical`` is True iff schemas match AND every shared content table
    matches. A skipped table (e.g. its columns differ) counts as NOT
    identical — the difference is real, just reported via the schema section.
    """

    db_a: Path
    db_b: Path
    tables_only_in_a: tuple[str, ...] = ()
    tables_only_in_b: tuple[str, ...] = ()
    indexes_only_in_a: tuple[str, ...] = ()
    indexes_only_in_b: tuple[str, ...] = ()
    # (index_name, a_sql, b_sql) for indexes present in both with different SQL.
    index_mismatches: tuple[tuple[str, str, str], ...] = ()
    views_only_in_a: tuple[str, ...] = ()
    views_only_in_b: tuple[str, ...] = ()
    # (view_name, a_sql, b_sql) for views present in both with different SQL.
    view_mismatches: tuple[tuple[str, str, str], ...] = ()
    column_diffs: tuple[ColumnDiff, ...] = ()
    table_results: tuple[TableContentResult, ...] = ()
    # column name lists for shared tables, for rendering SampleRow values.
    table_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)
    # Whether the two DBs are catalogs whose ids compare by natural key (module
    # docstring); otherwise rows compare raw, ids included.
    natural_keys: bool = False

    @property
    def schema_differs(self) -> bool:
        return bool(
            self.tables_only_in_a
            or self.tables_only_in_b
            or self.indexes_only_in_a
            or self.indexes_only_in_b
            or self.index_mismatches
            or self.views_only_in_a
            or self.views_only_in_b
            or self.view_mismatches
            or any(cd.differs for cd in self.column_diffs)
        )

    @property
    def content_differs(self) -> bool:
        return any(not r.identical for r in self.table_results)

    @property
    def identical(self) -> bool:
        return not (self.schema_differs or self.content_differs)


# --------------------------------------------------------------------------
# Connection + schema introspection
# --------------------------------------------------------------------------


def _rows(conn: sqlite3.Connection, sql: str) -> list[sqlite3.Row]:
    """``sql``'s rows, readable by column name."""
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    return cur.execute(sql).fetchall()


@dataclass(frozen=True)
class _Schema:
    # table name -> tuple of column defs (name, type, notnull, dflt, pk) in
    # cid order. Covers regular, virtual, and FTS-shadow tables (not sqlite_*).
    tables: dict[str, tuple[tuple[str, str, int, str | None, int], ...]]
    # table name -> normalized CREATE sql (whitespace-collapsed). Carries the
    # full definition table_info drops: table-level UNIQUE/CHECK/FK constraints,
    # WITHOUT ROWID, and FTS5 virtual-table options.
    table_sql: dict[str, str]
    # index name -> CREATE sql ("" for auto-indexes, which have NULL sql; their
    # defining constraint lives in the owning table's table_sql instead).
    indexes: dict[str, str]
    # view name -> normalized CREATE sql. Views are schema objects, not content
    # tables, so dbdiff compares definitions here and never row-fingerprints them.
    views: dict[str, str]
    virtual_tables: frozenset[str]
    shadow_tables: frozenset[str]


def _read_schema(conn: sqlite3.Connection, prefix: str) -> _Schema:
    """The schema of the DB attached as ``prefix``."""
    master = _rows(
        conn,
        f"SELECT type, name, sql FROM {prefix}.sqlite_master "
        "WHERE type IN ('table', 'index', 'view')",
    )
    table_names = [
        r["name"]
        for r in master
        if r["type"] == "table" and not r["name"].startswith("sqlite_")
    ]
    virtual = frozenset(
        r["name"]
        for r in master
        if r["type"] == "table"
        and r["sql"]
        and r["sql"].lstrip().upper().startswith("CREATE VIRTUAL TABLE")
    )
    shadow = frozenset(
        name
        for name in table_names
        if any(name == f"{vt}_{suf}" for vt in virtual for suf in _FTS_SHADOW_SUFFIXES)
    )
    tables = {name: _table_info(conn, prefix, name) for name in table_names}
    table_sql = {
        r["name"]: _normalize_sql(r["sql"])
        for r in master
        if r["type"] == "table" and r["name"] in set(table_names)
    }
    indexes = {
        r["name"]: (r["sql"] or "")
        for r in master
        if r["type"] == "index" and not r["name"].startswith("sqlite_stat")
    }
    views = {
        r["name"]: _normalize_sql(r["sql"])
        for r in master
        if r["type"] == "view" and not r["name"].startswith("sqlite_")
    }
    return _Schema(tables, table_sql, indexes, views, virtual, shadow)


def _normalize_sql(sql: str | None) -> str:
    """Collapse runs of whitespace between tokens in a CREATE statement, so
    reindentation / reflowing does not register as a schema difference, while
    any token change (a constraint, an FTS option, a renamed column) still
    does. (Token-adjacency spacing like ``(a`` vs ``( a`` is preserved — for
    the rebuild gate both DBs share one DDL string, so it never differs.)"""
    return " ".join((sql or "").split())


def _table_info(
    conn: sqlite3.Connection, prefix: str, table: str
) -> tuple[tuple[str, str, int, str | None, int], ...]:
    return tuple(
        (r["name"], r["type"], r["notnull"], r["dflt_value"], r["pk"])
        for r in _rows(conn, f"PRAGMA {prefix}.table_info({_quote(table)})")
    )


def _content_tables(schema: _Schema) -> list[str]:
    """User content tables: regular tables only (no sqlite_*, no FTS virtual
    or shadow tables — their content is derived; see module docstring)."""
    return sorted(
        name
        for name in schema.tables
        if name not in schema.virtual_tables and name not in schema.shadow_tables
    )


# --------------------------------------------------------------------------
# Natural-key projection of surrogate ids
# --------------------------------------------------------------------------


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _is_catalog(schema_a: _Schema, schema_b: _Schema) -> bool:
    """Whether both DBs are catalogs, so ids compare by the explicit map.
    Any other DB (a docs DB, a test fixture) compares raw rows."""
    return all(set(_IDS) <= set(s.tables) for s in (schema_a, schema_b))


def _check_catalog(conn: sqlite3.Connection, prefix: str, schema: _Schema) -> None:
    """Refuse the catalog attached as ``prefix`` when the map does not describe
    it, or its ids the data breaks, with a ValueError naming it (exit 2). Never a
    guess, never a raw-id fallback."""
    tables = set(_content_tables(schema))

    def has(table: str, column: str) -> bool:
        return table in tables and any(c[0] == column for c in schema.tables[table])

    for table, (id_column, key) in _IDS.items():
        for column in (id_column, *(key or ())):
            if not has(table, column):
                raise ValueError(f"the id map names {table}.{column}, which is missing")
    for (table, column), target in _REFERENCES.items():
        if not has(table, column):
            raise ValueError(f"the id map names {table}.{column}, which is missing")
        if _IDS[target][1] is None:
            raise ValueError(f"{table}.{column} references {target}, which has no key")
    # Every INTEGER PRIMARY KEY and INTEGER foreign key is an id the map covers.
    for table in sorted(tables):
        info = schema.tables[table]
        pk = [c[0] for c in info if c[4]]
        candidates = set(pk if len(pk) == 1 else ())
        candidates |= {
            fk["from"]
            for fk in _rows(conn, f"PRAGMA {prefix}.foreign_key_list({_quote(table)})")
        }
        for column in sorted(candidates):
            declared = next(c[1] for c in info if c[0] == column).upper()
            covered = (
                _IDS.get(table, ("",))[0] == column
                or (table, column) in _REFERENCES
                or (table, column) in _COMPARE_RAW
            )
            if declared == "INTEGER" and not covered:
                raise ValueError(
                    f"{table}.{column} is an INTEGER id column the id map does not "
                    "cover; add it to _IDS, _REFERENCES or _COMPARE_RAW"
                )
    for table, (id_column, key) in _IDS.items():
        source = f"{prefix}.{_quote(table)}"
        if conn.execute(
            f"SELECT 1 FROM {source} "
            f"WHERE typeof({_quote(id_column)}) != 'integer' LIMIT 1"
        ).fetchone():
            raise ValueError(f"{table}.{id_column} holds NULL or non-integer ids")
        if key is None:
            continue
        cols = ", ".join(_quote(c) for c in key)
        (repeated,) = conn.execute(
            f"SELECT (SELECT COUNT(*) FROM {source}) "
            f"- (SELECT COUNT(*) FROM (SELECT DISTINCT {cols} FROM {source}))"
        ).fetchone()
        if repeated:
            raise ValueError(
                f"{table}'s natural key ({', '.join(key)}) is not unique: "
                f"{repeated} row(s) repeat a key"
            )
    # A reference holds an id or nothing; anything else (text, a REAL) is no id.
    by_table: dict[str, list[str]] = {}
    for table, column in _REFERENCES:
        by_table.setdefault(table, []).append(column)
    for table, columns in sorted(by_table.items()):
        for (column,) in conn.execute(
            " UNION ALL ".join(
                f"SELECT {c!r} FROM (SELECT 1 FROM {prefix}.{_quote(table)} "
                f"WHERE typeof({_quote(c)}) NOT IN ('integer', 'null') LIMIT 1)"
                for c in sorted(columns)
            )
        ):
            raise ValueError(f"{table}.{column} holds non-integer references")


def _projected(table: str, columns: Sequence[str], catalog: bool) -> tuple[str, ...]:
    """``table``'s compared columns: in a catalog, all but its own id."""
    if not catalog or table not in _IDS:
        return tuple(columns)
    return tuple(c for c in columns if c != _IDS[table][0])


def _map_name(prefix: str, table: str) -> str:
    return _quote(f"_dbdiff_nk_{prefix}_{table}")


# The tags of a compared reference. A reference is a JSON array whose first
# element is one of these, and a data value is never an array, so a reference
# cannot equal data or another kind of reference: ["k", key] resolved to the
# row's natural key, ["d", id] an id no row holds (dangling). NULL stays NULL.
_RESOLVED, _DANGLING = "k", "d"


def _link_sql(prefix: str, table: str, column: str) -> str:
    """The tagged reference ``t.column`` holds, as JSON text (NULL for NULL)."""
    target = _REFERENCES[table, column]
    col = f"t.{_quote(column)}"
    return (
        f"CASE WHEN {col} IS NULL THEN NULL ELSE COALESCE("
        f"(SELECT nk FROM temp.{_map_name(prefix, target)} WHERE id = {col}), "
        f"json_array('{_DANGLING}', {col})) END"
    )


def _projection_sql(
    prefix: str, table: str, columns: Sequence[str], catalog: bool
) -> list[str]:
    return [
        _link_sql(prefix, table, c)
        if catalog and (table, c) in _REFERENCES
        else f"t.{_quote(c)}"
        for c in _projected(table, columns, catalog)
    ]


def _build_key_maps(work: sqlite3.Connection, prefix: str) -> None:
    """Materialize, per keyed `_IDS` table, its id -> tagged reference
    ``["k", key]`` in the temp store, so a lookup is the compared text as it is.
    A key is always a JSON array, so a NULL key column is JSON null; a data value
    in it is a JSON scalar (a BLOB a {"blob": hex} object) and a reference a
    tagged array. The map is acyclic, so the recursion ends."""
    built: set[str] = set()

    def build(table: str) -> None:
        if table in built:
            return
        id_column, key = _IDS[table]
        parts = []
        for c in key or ():
            if (table, c) in _REFERENCES:
                build(_REFERENCES[table, c])
                parts.append(f"json({_link_sql(prefix, table, c)})")
            else:
                col = f"t.{_quote(c)}"
                parts.append(
                    f"CASE WHEN typeof({col}) = 'blob' "
                    f"THEN json_object('blob', hex({col})) ELSE {col} END"
                )
        name = _map_name(prefix, table)
        work.execute(
            f"CREATE TEMP TABLE {name} (id INTEGER PRIMARY KEY, nk TEXT NOT NULL)"
        )
        work.execute(
            f"INSERT INTO temp.{name} SELECT t.{_quote(id_column)}, "
            f"json_array('{_RESOLVED}', json_array({', '.join(parts)})) "
            f"FROM {prefix}.{_quote(table)} AS t"
        )
        built.add(table)

    for table in sorted(set(_REFERENCES.values())):
        build(table)


# --------------------------------------------------------------------------
# Row canonicalization + streaming multiset fingerprint
# --------------------------------------------------------------------------


def _row_hash(values: Sequence[object]) -> int:
    """Type-discriminated, length-prefixed BLAKE2b-128 of a row, as an int.

    Each value is tagged by storage class so an int never collides with the
    text of the same digits, and length-prefixed so column boundaries are
    unambiguous (``("a", "b")`` cannot collide with ``("ab", "")``).
    """
    h = hashlib.blake2b(digest_size=_HASH_BYTES)
    for v in values:
        if v is None:
            tag, payload = b"\x00", b""
        elif isinstance(v, bool):
            # bool is an int subclass; tag it distinctly to be explicit.
            tag, payload = b"\x05", b"\x01" if v else b"\x00"
        elif isinstance(v, int):
            tag, payload = b"\x01", str(v).encode("utf-8")
        elif isinstance(v, float):
            tag, payload = b"\x02", struct.pack(">d", v)
        elif isinstance(v, str):
            tag, payload = b"\x03", v.encode("utf-8")
        elif isinstance(v, (bytes, bytearray, memoryview)):
            tag, payload = b"\x04", bytes(v)
        else:  # pragma: no cover - sqlite3 yields only the types above
            raise TypeError(f"uncanonicalizable value of type {type(v)!r}")
        h.update(len(payload).to_bytes(8, "big"))
        h.update(tag)
        h.update(payload)
    return int.from_bytes(h.digest(), "big")


def _select_sql(
    prefix: str,
    table: str,
    columns: Sequence[str],
    catalog: bool,
    skip_where: str | None,
    tag: str = "",
) -> str:
    """``table``'s projected rows in DB ``prefix``, each column aliased by name
    (``skip_where`` names the stored columns, which ``t`` alone carries)."""
    cols = ", ".join(
        f"{expr} AS {_quote(c)}"
        for expr, c in zip(
            _projection_sql(prefix, table, columns, catalog),
            _projected(table, columns, catalog),
        )
    )
    sql = f"SELECT {cols}{tag} FROM {prefix}.{_quote(table)} AS t"
    if skip_where:
        sql += f" WHERE NOT ({skip_where})"
    return sql


def _fingerprint(
    work: sqlite3.Connection,
    prefix: str,
    table: str,
    columns: Sequence[str],
    catalog: bool,
    skip_where: str | None,
    *,
    batch: int = 20_000,
) -> tuple[int, str]:
    """Stream ``table``'s projection and return (row_count, fingerprint_hex).

    O(1) memory: one cursor fetched in batches, a running count and a running
    sum-of-row-hashes mod 2**128. Order-independent (sum is commutative) and
    multiplicity-sensitive.
    """
    cur = work.execute(_select_sql(prefix, table, columns, catalog, skip_where))
    total = 0
    count = 0
    while True:
        rows = cur.fetchmany(batch)
        if not rows:
            break
        for row in rows:
            total = (total + _row_hash(row)) % _FP_MODULUS
            count += 1
    return count, f"{total:0{_HASH_BYTES * 2}x}"


# --------------------------------------------------------------------------
# Actionable row dump (multiset difference via ATTACH)
# --------------------------------------------------------------------------


def _diff_samples(
    work: sqlite3.Connection,
    table: str,
    stored: Sequence[str],
    catalog: bool,
    skip_where: str | None,
    limit: int,
) -> tuple[tuple[SampleRow, ...], tuple[SampleRow, ...]]:
    """Return (rows in A not B, rows in B not A), each capped at ``limit``.

    Uses ``SELECT ... 1 FROM a UNION ALL SELECT ... -1 FROM b`` grouped by all
    compared columns; ``SUM`` of the +1/-1 tags gives the net multiplicity per
    distinct row. Net > 0 → A has extra copies; net < 0 → B does. The GROUP BY
    runs in SQLite with ``PRAGMA temp_store = FILE``, so the working set spills
    to an on-disk temp file rather than RAM, keeping Python memory flat even for
    multi-million-row tables. NULLs group together and BLOBs compare by bytes,
    both of which match the fingerprint's NULL-aware/byte semantics.
    """
    columns = _projected(table, stored, catalog)
    cols = ", ".join(_quote(c) for c in columns)
    a_rows = _select_sql("a", table, stored, catalog, skip_where, ", 1 AS _dbdiff_src")
    b_rows = _select_sql("b", table, stored, catalog, skip_where, ", -1 AS _dbdiff_src")
    union = f"{a_rows} UNION ALL {b_rows}"
    base = (
        f"SELECT {cols}, SUM(_dbdiff_src) AS _dbdiff_net "
        f"FROM ({union}) GROUP BY {cols} "
        f"HAVING _dbdiff_net {{op}} 0 ORDER BY {cols} LIMIT ?"
    )
    ncols = len(columns)
    a_not_b = tuple(
        SampleRow(net=row[ncols], values=tuple(row[:ncols]))
        for row in work.execute(base.format(op=">"), (limit,))
    )
    b_not_a = tuple(
        SampleRow(net=row[ncols], values=tuple(row[:ncols]))
        for row in work.execute(base.format(op="<"), (limit,))
    )
    return a_not_b, b_not_a


def _work_connection(db_a: Path, db_b: Path) -> sqlite3.Connection:
    """A private in-memory connection with both DBs attached read-only: the one
    handle the whole run reads through (schema, catalog check, natural-key maps,
    fingerprints, multiset difference), inside one read transaction, so a DB
    replaced or written mid-run cannot pair one version's schema with another's
    content. Its temp store spills to disk (the default is build-dependent and
    may be MEMORY), so Python and SQLite memory stay flat on multi-million-row
    tables. Raises FileNotFoundError for a missing path (a `mode=ro` URI to one
    would otherwise raise an opaque OperationalError)."""
    for path in (db_a, db_b):
        if not path.exists():
            raise FileNotFoundError(f"DB not found: {path}")
    conn = sqlite3.connect(
        "file::memory:?cache=private", uri=True, isolation_level=None
    )
    try:
        conn.execute("PRAGMA temp_store = FILE")
        conn.execute("ATTACH DATABASE ? AS a", (f"file:{db_a}?mode=ro",))
        conn.execute("ATTACH DATABASE ? AS b", (f"file:{db_b}?mode=ro",))
        conn.execute("BEGIN")
        # Read both now, so the transaction holds each DB's snapshot from here on.
        conn.execute("SELECT count(*) FROM a.sqlite_master").fetchone()
        conn.execute("SELECT count(*) FROM b.sqlite_master").fetchone()
    except BaseException:
        conn.close()
        raise
    return conn


# --------------------------------------------------------------------------
# Top-level comparison
# --------------------------------------------------------------------------


def diff_db_content(
    db_a: str | Path,
    db_b: str | Path,
    *,
    ignore: dict[str, TableIgnore] | None = None,
    sample_rows: int = 10,
) -> DiffReport:
    """Compare the *content* of two reg_meta SQLite DBs.

    Args:
        db_a, db_b: paths to the two DB files (opened read-only).
        ignore: per-table exclusions. ``None`` uses ``DEFAULT_IGNORE`` (drops
            only `import_manifest.import_date`). Pass ``{}`` to compare
            everything, including the build timestamp.
        sample_rows: max differing rows to capture per direction per table.

    Returns a :class:`DiffReport`; never raises on content differences (they
    are reported in the result). Raises FileNotFoundError if a path is missing,
    and ValueError naming the reason if a catalog fails `_check_catalog`.
    """
    db_a = Path(db_a)
    db_b = Path(db_b)
    ignore = DEFAULT_IGNORE if ignore is None else ignore

    work = _work_connection(db_a, db_b)
    try:
        schema_a = _read_schema(work, "a")
        schema_b = _read_schema(work, "b")
        catalog = _is_catalog(schema_a, schema_b)
        if catalog:
            _check_catalog(work, "a", schema_a)
            _check_catalog(work, "b", schema_b)
        report = DiffReport(db_a=db_a, db_b=db_b, natural_keys=catalog)
        _compare_schema(schema_a, schema_b, report)
        if catalog:
            _build_key_maps(work, "a")
            _build_key_maps(work, "b")
        _compare_content(work, schema_a, schema_b, catalog, ignore, sample_rows, report)
    finally:
        work.close()
    return report


def _compare_schema(schema_a: _Schema, schema_b: _Schema, report: DiffReport) -> None:
    a_tables = set(schema_a.tables)
    b_tables = set(schema_b.tables)
    report.tables_only_in_a = tuple(sorted(a_tables - b_tables))
    report.tables_only_in_b = tuple(sorted(b_tables - a_tables))

    column_diffs: list[ColumnDiff] = []
    for table in sorted(a_tables & b_tables):
        cols_a = {c[0]: c for c in schema_a.tables[table]}
        cols_b = {c[0]: c for c in schema_b.tables[table]}
        only_a = tuple(c for c in cols_a if c not in cols_b)
        only_b = tuple(c for c in cols_b if c not in cols_a)
        mismatched = tuple(
            (name, _fmt_coldef(cols_a[name]), _fmt_coldef(cols_b[name]))
            for name in cols_a
            if name in cols_b and cols_a[name] != cols_b[name]
        )
        sql_a = schema_a.table_sql.get(table, "")
        sql_b = schema_b.table_sql.get(table, "")
        cd = ColumnDiff(
            table,
            only_a,
            only_b,
            mismatched,
            create_sql_a=sql_a if sql_a != sql_b else "",
            create_sql_b=sql_b if sql_a != sql_b else "",
        )
        if cd.differs:
            column_diffs.append(cd)
    report.column_diffs = tuple(column_diffs)

    a_idx = set(schema_a.indexes)
    b_idx = set(schema_b.indexes)
    report.indexes_only_in_a = tuple(sorted(a_idx - b_idx))
    report.indexes_only_in_b = tuple(sorted(b_idx - a_idx))
    report.index_mismatches = tuple(
        (name, schema_a.indexes[name], schema_b.indexes[name])
        for name in sorted(a_idx & b_idx)
        if schema_a.indexes[name] != schema_b.indexes[name]
    )

    a_views = set(schema_a.views)
    b_views = set(schema_b.views)
    report.views_only_in_a = tuple(sorted(a_views - b_views))
    report.views_only_in_b = tuple(sorted(b_views - a_views))
    report.view_mismatches = tuple(
        (name, schema_a.views[name], schema_b.views[name])
        for name in sorted(a_views & b_views)
        if schema_a.views[name] != schema_b.views[name]
    )


def _fmt_coldef(coldef: tuple[str, str, int, str | None, int]) -> str:
    name, ctype, notnull, dflt, pk = coldef
    parts = [f"{name} {ctype or '<none>'}"]
    if notnull:
        parts.append("NOT NULL")
    if dflt is not None:
        parts.append(f"DEFAULT {dflt}")
    if pk:
        parts.append(f"PK({pk})")
    return " ".join(parts)


def _compare_content(
    work: sqlite3.Connection,
    schema_a: _Schema,
    schema_b: _Schema,
    catalog: bool,
    ignore: dict[str, TableIgnore],
    sample_rows: int,
    report: DiffReport,
) -> None:
    tables_a = set(_content_tables(schema_a))
    tables_b = set(_content_tables(schema_b))
    results: list[TableContentResult] = []
    for table in sorted(tables_a & tables_b):
        spec = ignore.get(table, TableIgnore())
        cols_a = [c[0] for c in schema_a.tables[table]]
        cols_b = [c[0] for c in schema_b.tables[table]]
        columns = _projected(table, cols_a, catalog)
        report.table_columns[table] = columns

        if cols_a != cols_b:
            # Column set/order differs (already in the schema section). A
            # multiset fingerprint over mismatched columns is meaningless.
            results.append(
                TableContentResult(
                    table=table,
                    count_a=-1,
                    count_b=-1,
                    fingerprint_a="",
                    fingerprint_b="",
                    columns=(),
                    skipped_reason="columns differ (see schema diff)",
                )
            )
            continue

        count_a, fp_a = _fingerprint(work, "a", table, cols_a, catalog, spec.skip_where)
        count_b, fp_b = _fingerprint(work, "b", table, cols_a, catalog, spec.skip_where)

        result = TableContentResult(
            table=table,
            count_a=count_a,
            count_b=count_b,
            fingerprint_a=fp_a,
            fingerprint_b=fp_b,
            columns=columns,
            link_columns=tuple(
                c for c in columns if catalog and (table, c) in _REFERENCES
            ),
        )
        if not result.identical:
            a_not_b, b_not_a = _diff_samples(
                work, table, cols_a, catalog, spec.skip_where, sample_rows
            )
            note = None
            if not a_not_b and not b_not_a:
                # Fingerprints differ but the value-level multiset diff is
                # empty. SQLite's GROUP BY treats numerically-equal values of
                # different storage classes (e.g. 1 and 1.0) as one group,
                # while the type-tagged fingerprint does not — so this signals
                # a storage-class/type difference, not a value difference.
                note = (
                    "fingerprint differs but value-level diff is empty; "
                    "likely a storage-class/type difference"
                )
            result = replace(
                result, sample_a_not_b=a_not_b, sample_b_not_a=b_not_a, note=note
            )
        results.append(result)
    report.table_results = tuple(results)


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------


def _fmt_cell(value: object, width: int = 80) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        head = raw[:16].hex()
        ellipsis = "…" if len(raw) > 16 else ""
        return f"<blob {len(raw)}B 0x{head}{ellipsis}>"
    text = value if isinstance(value, str) else str(value)
    if len(text) > width:
        return f"{text[:width]}… (+{len(text) - width} chars)"
    return text


def _fmt_sample(rows: Iterable[SampleRow], result: TableContentResult) -> list[str]:
    lines: list[str] = []
    for row in rows:
        sign = f"+{row.net}" if row.net > 0 else str(row.net)
        cells = ", ".join(
            f"{col}={_fmt_cell(_text(_display(val, link=col in result.link_columns)))}"
            for col, val in zip(result.columns, row.values)
        )
        lines.append(f"      ({sign}) {cells}")
    return lines


def format_report(report: DiffReport) -> str:
    """Render a human-readable report. Identical DBs get a one-line summary;
    differences are itemized table-by-table with sample rows."""
    out: list[str] = []
    out.append(f"Comparing content:\n  A: {report.db_a}\n  B: {report.db_b}")
    if report.natural_keys:
        out.append("  catalog ids compare by natural key")

    if report.identical:
        n = len(report.table_results)
        out.append(f"\n[IDENTICAL] schema matches; {n} content table(s) match.")
        return "\n".join(out)

    out.append("\n[DIFFERENCES FOUND]")

    if report.schema_differs:
        out.append("\n== schema ==")
        for t in report.tables_only_in_a:
            out.append(f"  table only in A: {t}")
        for t in report.tables_only_in_b:
            out.append(f"  table only in B: {t}")
        for cd in report.column_diffs:
            for c in cd.only_in_a:
                out.append(f"  {cd.table}: column only in A: {c}")
            for c in cd.only_in_b:
                out.append(f"  {cd.table}: column only in B: {c}")
            for name, a_def, b_def in cd.mismatched:
                out.append(f"  {cd.table}.{name}: A[{a_def}] != B[{b_def}]")
            if cd.definition_differs:
                out.append(
                    f"  {cd.table}: table definition differs (constraints/options)"
                )
                out.append(f"      A: {cd.create_sql_a}")
                out.append(f"      B: {cd.create_sql_b}")
        for i in report.indexes_only_in_a:
            out.append(f"  index only in A: {i}")
        for i in report.indexes_only_in_b:
            out.append(f"  index only in B: {i}")
        for name, a_sql, b_sql in report.index_mismatches:
            out.append(f"  index {name}: SQL differs")
            out.append(f"      A: {a_sql}")
            out.append(f"      B: {b_sql}")
        for v in report.views_only_in_a:
            out.append(f"  view only in A: {v}")
        for v in report.views_only_in_b:
            out.append(f"  view only in B: {v}")
        for name, a_sql, b_sql in report.view_mismatches:
            out.append(f"  view {name}: SQL differs")
            out.append(f"      A: {a_sql}")
            out.append(f"      B: {b_sql}")

    differing = [r for r in report.table_results if not r.identical]
    if differing:
        out.append("\n== content ==")
        for r in differing:
            if r.skipped_reason is not None:
                out.append(f"  {r.table}: SKIPPED — {r.skipped_reason}")
                continue
            delta = r.count_b - r.count_a
            out.append(
                f"  {r.table}: rows A={r.count_a:,} B={r.count_b:,} "
                f"(Δ={delta:+,}); fingerprint A={r.fingerprint_a[:12]}… "
                f"B={r.fingerprint_b[:12]}…"
            )
            if r.note:
                out.append(f"    note: {r.note}")
            if r.sample_a_not_b:
                out.append(f"    rows in A not B (first {len(r.sample_a_not_b)}):")
                out.extend(_fmt_sample(r.sample_a_not_b, r))
            if r.sample_b_not_a:
                out.append(f"    rows in B not A (first {len(r.sample_b_not_a)}):")
                out.extend(_fmt_sample(r.sample_b_not_a, r))

    matched = sum(1 for r in report.table_results if r.identical)
    out.append(f"\n{matched}/{len(report.table_results)} content table(s) matched.")
    return "\n".join(out)


def _display(value: object, *, link: bool = False) -> object:
    """A reported sample value as JSON: the one path every reported value takes,
    in the JSON and the text report. The diff compares the stored values; this is
    for display only.

    A data value keeps its SQLite storage class: NULL is null, INTEGER and REAL
    stay numbers, TEXT is a string (long text truncated) and a BLOB is a
    ``{"blob": preview}`` object. Integer 1, text '1', blob b'1' and NULL are
    different rows to the diff, so they must not print alike. A non-finite REAL
    has no JSON number, so it is a ``{"real": "inf" | "-inf" | "nan"}`` object
    rather than the invalid ``Infinity``/``NaN`` or the string 'inf'.

    A reference (``link``) is decoded from its tagged JSON: a resolved one prints
    its natural key (a one-column key as its value), and a dangling id
    ``{"dangling_id": id}``. Its values go through this same
    path, so a REAL infinity inside a key prints as ``{"real": "inf"}`` too."""
    if link and isinstance(value, str):
        return _untag(json.loads(value))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"blob": _fmt_cell(value)}
    if isinstance(value, float) and not math.isfinite(value):
        return {"real": str(value)}
    if isinstance(value, str):
        return _fmt_cell(value, width=200)
    if isinstance(value, dict):  # a BLOB inside a key: {"blob": hex}
        return {k: _display(v) for k, v in value.items()}
    return value


def _untag(reference: list) -> object:
    tag, payload = reference
    if tag == _DANGLING:
        return {"dangling_id": _display(payload)}
    # In a key, a data value is a JSON scalar or object and a reference an array.
    parts = [_untag(p) if isinstance(p, list) else _display(p) for p in payload]
    return parts[0] if len(parts) == 1 else parts


def _text(value: object) -> object:
    """A displayed value for the text report as compact JSON, so it keeps its
    storage class as `--json` does: text '1' prints quoted, integer 1 bare. NULL
    stays None and prints as NULL."""
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _report_to_dict(report: DiffReport) -> dict[str, object]:
    """JSON-serializable view; every sample value goes through ``_display``."""

    def cells(
        rows: Iterable[SampleRow], result: TableContentResult
    ) -> list[dict[str, object]]:
        return [
            {
                "net": row.net,
                "row": {
                    c: _display(v, link=c in result.link_columns)
                    for c, v in zip(result.columns, row.values)
                },
            }
            for row in rows
        ]

    return {
        "db_a": str(report.db_a),
        "db_b": str(report.db_b),
        "identical": report.identical,
        "natural_keys": report.natural_keys,
        "schema": {
            "tables_only_in_a": list(report.tables_only_in_a),
            "tables_only_in_b": list(report.tables_only_in_b),
            "indexes_only_in_a": list(report.indexes_only_in_a),
            "indexes_only_in_b": list(report.indexes_only_in_b),
            "index_mismatches": [m[0] for m in report.index_mismatches],
            "views_only_in_a": list(report.views_only_in_a),
            "views_only_in_b": list(report.views_only_in_b),
            "view_mismatches": [m[0] for m in report.view_mismatches],
            "column_diffs": [
                {
                    "table": cd.table,
                    "only_in_a": list(cd.only_in_a),
                    "only_in_b": list(cd.only_in_b),
                    "mismatched": [m[0] for m in cd.mismatched],
                    "definition_differs": cd.definition_differs,
                }
                for cd in report.column_diffs
            ],
        },
        "tables": [
            {
                "table": r.table,
                "identical": r.identical,
                "count_a": r.count_a,
                "count_b": r.count_b,
                "fingerprint_a": r.fingerprint_a,
                "fingerprint_b": r.fingerprint_b,
                "skipped_reason": r.skipped_reason,
                "note": r.note,
                "sample_a_not_b": cells(r.sample_a_not_b, r),
                "sample_b_not_a": cells(r.sample_b_not_a, r),
            }
            for r in report.table_results
        ],
    }


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m reg_meta_build.dbdiff",
        description=(
            "Content-level diff of two reg_meta SQLite DBs (order-independent, "
            "BLOB-safe). Exit 0 if identical, 1 if they differ, 2 on error."
        ),
    )
    parser.add_argument("db_a", type=Path, help="first DB file")
    parser.add_argument("db_b", type=Path, help="second DB file")
    parser.add_argument(
        "-n",
        "--sample-rows",
        type=int,
        default=10,
        metavar="N",
        help="max differing rows to dump per direction per table (default: 10)",
    )
    ignores = parser.add_mutually_exclusive_group()
    ignores.add_argument(
        "--no-default-ignore",
        action="store_true",
        help="compare every column/row, including import_manifest.import_date",
    )
    # The Rust port's gate (PORT.md); deleted with the port at cutover.
    ignores.add_argument(
        "--ignore-file",
        type=Path,
        metavar="JSON",
        help=(
            "instead of the default, ignore the import_manifest keys a file lists as "
            '{"import_manifest": {"<key>": "<reason>", ...}}'
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the report as JSON instead of text",
    )
    return parser


def _load_ignore_file(path: Path) -> dict[str, TableIgnore]:
    """The `import_manifest` keys a port ignore file lists, each with its reason."""
    document = json.loads(path.read_text(encoding="utf-8"))
    keys = document.get("import_manifest") if isinstance(document, dict) else None
    if (
        set(document) != {"import_manifest"}
        or not isinstance(keys, dict)
        or not keys
        or not all(
            isinstance(key, str) and key and isinstance(reason, str) and reason.strip()
            for key, reason in keys.items()
        )
    ):
        raise ValueError(
            f'{path}: expected {{"import_manifest": {{"<key>": "<reason>", ...}}}}'
        )
    quoted = ", ".join("'" + key.replace("'", "''") + "'" for key in sorted(keys))
    return {"import_manifest": TableIgnore(skip_where=f"key IN ({quoted})")}


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    ignore: dict[str, TableIgnore] = {} if args.no_default_ignore else DEFAULT_IGNORE
    try:
        if args.ignore_file is not None:
            ignore = _load_ignore_file(args.ignore_file)
        report = diff_db_content(
            args.db_a, args.db_b, ignore=ignore, sample_rows=args.sample_rows
        )
    except (FileNotFoundError, ValueError, sqlite3.Error) as exc:
        print(f"dbdiff: error: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(
            json.dumps(
                _report_to_dict(report), indent=2, ensure_ascii=False, allow_nan=False
            )
        )
    else:
        print(format_report(report))
    return 0 if report.identical else 1


if __name__ == "__main__":
    raise SystemExit(main())
