"""Content-level diff for two reg_meta SQLite DBs.

This is the acceptance gate for the Model A migration's adapter refactor
(`MIGRATION_PLAN.md` stage A4.1): A4.1 moves SCB ingest out of
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
   names/types/order/NOT NULL/default/PK, plus the same indexes (named and
   auto). Tables/columns/indexes present in only one DB are reported.

2. **Per-table content compare, order- and BLOB-safe** — for every user
   content table, compare row COUNT and an order-independent *multiset*
   fingerprint. Each row is canonicalized to a type-tagged byte string
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
   (an ISO timestamp). `schema_version` and every content/ID column are
   compared. `input_dir` (an absolute path) is *not* ignored by default —
   it is identical for a same-machine rebuild (the A4.1 case); a
   cross-machine comparison can add it explicitly. There is no other
   build/provenance table in the shipped DB (`build_manifest` is scaffolded
   but unpopulated and absent here). Pass `ignore={}` to compare everything.

4. **Actionable mismatch output** — a mismatch names the table, the count
   delta, and the first N differing rows in each direction (present-in-A,
   present-in-B), found via an ATTACH-based multiset-difference query that
   offloads its working set to SQLite's on-disk temp store. `DiffReport`
   carries the structured result; the CLI exits non-zero on any difference.

FTS5 handling: the three `*_fts` virtual tables are external-content
(`content='variable'` etc.), so their queryable content is a pure
projection of base tables that ARE compared. Their shadow tables
(`*_fts_data/_idx/_docsize/_config`) hold the serialized inverted index,
whose bytes depend on insert order — comparing them would false-positive on
an emit-order change that left content identical. So FTS virtual + shadow
tables are included in the *schema* comparison (structure must match) but
excluded from the *content* comparison. `sqlite_*` internal tables
(`sqlite_sequence`, `sqlite_stat*`) are excluded throughout.

Canonicalization is type-discriminated: SQLite storage class (NULL /
INTEGER / REAL / TEXT / BLOB) is part of the fingerprint, so an ID that
turned from an int into a float would be flagged. The reg_meta schema is
float-free (only INTEGER/TEXT/BLOB columns), so this never produces a
spurious diff in practice; it just makes the gate strict.

This is a standalone, read-only tool — it opens both DBs with `mode=ro`
URIs and works on any two reg_meta DB files. It does not import or touch
the build pipeline.

CLI:  ``python -m reg_meta_build.dbdiff <db_a> <db_b> [-n N] [--no-default-ignore] [--json]``
Exit: 0 identical · 1 differs · 2 usage/IO error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import struct
import sys
from dataclasses import dataclass, field
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

    ``drop_columns`` are removed from the fingerprint and the row dump (the
    column still exists; its *values* are not compared). ``skip_where`` is a
    SQL boolean predicate identifying rows to exclude from BOTH DBs (applied
    as ``WHERE NOT (<skip_where>)``); it also lowers the compared row count,
    so an ignored row never shows up as a count delta.
    """

    drop_columns: frozenset[str] = frozenset()
    skip_where: str | None = None


# Default ignore set — deliberately minimal (see requirement 3 in the module
# docstring). Only `import_manifest.import_date` is nondeterministic.
DEFAULT_IGNORE: dict[str, TableIgnore] = {
    "import_manifest": TableIgnore(skip_where="key = 'import_date'"),
}


@dataclass(frozen=True)
class ColumnDiff:
    """Per-table schema difference."""

    table: str
    only_in_a: tuple[str, ...]
    only_in_b: tuple[str, ...]
    # (column, a_definition, b_definition) for columns present in both whose
    # type/order/notnull/default/pk differ.
    mismatched: tuple[tuple[str, str, str], ...]

    @property
    def differs(self) -> bool:
        return bool(self.only_in_a or self.only_in_b or self.mismatched)


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
    columns: tuple[str, ...]  # columns actually compared (post drop_columns)
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
    column_diffs: tuple[ColumnDiff, ...] = ()
    table_results: tuple[TableContentResult, ...] = ()
    # column name lists for shared tables, for rendering SampleRow values.
    table_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def schema_differs(self) -> bool:
        return bool(
            self.tables_only_in_a
            or self.tables_only_in_b
            or self.indexes_only_in_a
            or self.indexes_only_in_b
            or self.index_mismatches
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


def _connect_ro(path: Path) -> sqlite3.Connection:
    """Open ``path`` read-only. Raises FileNotFoundError if it is missing
    (a `mode=ro` URI to a nonexistent file would otherwise raise an opaque
    OperationalError)."""
    if not path.exists():
        raise FileNotFoundError(f"DB not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@dataclass(frozen=True)
class _Schema:
    # table name -> tuple of column defs (name, type, notnull, dflt, pk) in
    # cid order. Covers regular, virtual, and FTS-shadow tables (not sqlite_*).
    tables: dict[str, tuple[tuple[str, str, int, str | None, int], ...]]
    # index name -> CREATE sql ("" for auto-indexes, which have NULL sql).
    indexes: dict[str, str]
    virtual_tables: frozenset[str]
    shadow_tables: frozenset[str]


def _read_schema(conn: sqlite3.Connection) -> _Schema:
    master = conn.execute(
        "SELECT type, name, sql FROM sqlite_master WHERE type IN ('table', 'index')"
    ).fetchall()
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
    tables = {name: _table_info(conn, name) for name in table_names}
    indexes = {
        r["name"]: (r["sql"] or "")
        for r in master
        if r["type"] == "index" and not r["name"].startswith("sqlite_stat")
    }
    return _Schema(tables, indexes, virtual, shadow)


def _table_info(
    conn: sqlite3.Connection, table: str
) -> tuple[tuple[str, str, int, str | None, int], ...]:
    return tuple(
        (r["name"], r["type"], r["notnull"], r["dflt_value"], r["pk"])
        for r in conn.execute(f'PRAGMA table_info("{table}")')
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


def _select_sql(table: str, columns: Sequence[str], skip_where: str | None) -> str:
    cols = ", ".join(f'"{c}"' for c in columns)
    sql = f'SELECT {cols} FROM "{table}"'
    if skip_where:
        sql += f" WHERE NOT ({skip_where})"
    return sql


def _fingerprint(
    conn: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    skip_where: str | None,
    *,
    batch: int = 20_000,
) -> tuple[int, str]:
    """Stream ``table`` and return (row_count, fingerprint_hex).

    O(1) memory: one cursor fetched in batches, a running count and a running
    sum-of-row-hashes mod 2**128. Order-independent (sum is commutative) and
    multiplicity-sensitive.
    """
    cur = conn.execute(_select_sql(table, columns, skip_where))
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
    db_a: Path,
    db_b: Path,
    table: str,
    columns: Sequence[str],
    skip_where: str | None,
    limit: int,
) -> tuple[tuple[SampleRow, ...], tuple[SampleRow, ...]]:
    """Return (rows in A not B, rows in B not A), each capped at ``limit``.

    Uses ``SELECT ... 1 FROM a UNION ALL SELECT ... -1 FROM b`` grouped by all
    compared columns; ``SUM`` of the +1/-1 tags gives the net multiplicity per
    distinct row. Net > 0 → A has extra copies; net < 0 → B does. The GROUP BY
    runs in SQLite (spilling to its temp store), keeping Python memory flat
    even for multi-million-row tables. NULLs group together and BLOBs compare
    by bytes, both of which match the fingerprint's NULL-aware/byte semantics.
    """
    conn = sqlite3.connect("file::memory:?cache=private", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("ATTACH DATABASE ? AS a", (f"file:{db_a}?mode=ro",))
        conn.execute("ATTACH DATABASE ? AS b", (f"file:{db_b}?mode=ro",))
        cols = ", ".join(f'"{c}"' for c in columns)
        where = f" WHERE NOT ({skip_where})" if skip_where else ""
        union = (
            f'SELECT {cols}, 1 AS _dbdiff_src FROM a."{table}"{where} '
            f"UNION ALL "
            f'SELECT {cols}, -1 AS _dbdiff_src FROM b."{table}"{where}'
        )
        base = (
            f"SELECT {cols}, SUM(_dbdiff_src) AS _dbdiff_net "
            f"FROM ({union}) GROUP BY {cols} "
            f"HAVING _dbdiff_net {{op}} 0 ORDER BY {cols} LIMIT ?"
        )
        ncols = len(columns)
        a_not_b = tuple(
            SampleRow(net=row[ncols], values=tuple(row[:ncols]))
            for row in conn.execute(base.format(op=">"), (limit,))
        )
        b_not_a = tuple(
            SampleRow(net=row[ncols], values=tuple(row[:ncols]))
            for row in conn.execute(base.format(op="<"), (limit,))
        )
        return a_not_b, b_not_a
    finally:
        conn.close()


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
    are reported in the result). Raises FileNotFoundError if a path is missing.
    """
    db_a = Path(db_a)
    db_b = Path(db_b)
    ignore = DEFAULT_IGNORE if ignore is None else ignore

    conn_a = _connect_ro(db_a)
    conn_b = _connect_ro(db_b)
    try:
        schema_a = _read_schema(conn_a)
        schema_b = _read_schema(conn_b)
        report = DiffReport(db_a=db_a, db_b=db_b)
        _compare_schema(schema_a, schema_b, report)
        _compare_content(
            db_a, db_b, conn_a, conn_b, schema_a, schema_b, ignore, sample_rows, report
        )
        return report
    finally:
        conn_a.close()
        conn_b.close()


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
        cd = ColumnDiff(table, only_a, only_b, mismatched)
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
    db_a: Path,
    db_b: Path,
    conn_a: sqlite3.Connection,
    conn_b: sqlite3.Connection,
    schema_a: _Schema,
    schema_b: _Schema,
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
        report.table_columns[table] = tuple(cols_a)

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

        columns = [c for c in cols_a if c not in spec.drop_columns]
        report.table_columns[table] = tuple(columns)
        count_a, fp_a = _fingerprint(conn_a, table, columns, spec.skip_where)
        count_b, fp_b = _fingerprint(conn_b, table, columns, spec.skip_where)

        result = TableContentResult(
            table=table,
            count_a=count_a,
            count_b=count_b,
            fingerprint_a=fp_a,
            fingerprint_b=fp_b,
            columns=tuple(columns),
        )
        if not result.identical:
            a_not_b, b_not_a = _diff_samples(
                db_a, db_b, table, columns, spec.skip_where, sample_rows
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
            result = TableContentResult(
                table=table,
                count_a=count_a,
                count_b=count_b,
                fingerprint_a=fp_a,
                fingerprint_b=fp_b,
                columns=tuple(columns),
                sample_a_not_b=a_not_b,
                sample_b_not_a=b_not_a,
                note=note,
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


def _fmt_sample(rows: Iterable[SampleRow], columns: Sequence[str]) -> list[str]:
    lines: list[str] = []
    for row in rows:
        sign = f"+{row.net}" if row.net > 0 else str(row.net)
        cells = ", ".join(
            f"{col}={_fmt_cell(val)}" for col, val in zip(columns, row.values)
        )
        lines.append(f"      ({sign}) {cells}")
    return lines


def format_report(report: DiffReport) -> str:
    """Render a human-readable report. Identical DBs get a one-line summary;
    differences are itemized table-by-table with sample rows."""
    out: list[str] = []
    out.append(f"Comparing content:\n  A: {report.db_a}\n  B: {report.db_b}")

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
        for i in report.indexes_only_in_a:
            out.append(f"  index only in A: {i}")
        for i in report.indexes_only_in_b:
            out.append(f"  index only in B: {i}")
        for name, a_sql, b_sql in report.index_mismatches:
            out.append(f"  index {name}: SQL differs")
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
                out.extend(_fmt_sample(r.sample_a_not_b, r.columns))
            if r.sample_b_not_a:
                out.append(f"    rows in B not A (first {len(r.sample_b_not_a)}):")
                out.extend(_fmt_sample(r.sample_b_not_a, r.columns))

    matched = sum(1 for r in report.table_results if r.identical)
    out.append(f"\n{matched}/{len(report.table_results)} content table(s) matched.")
    return "\n".join(out)


def _report_to_dict(report: DiffReport) -> dict[str, object]:
    """JSON-serializable view (sample row values are rendered to strings so
    BLOBs/long text stay printable)."""

    def cells(
        rows: Iterable[SampleRow], cols: Sequence[str]
    ) -> list[dict[str, object]]:
        return [
            {
                "net": row.net,
                "row": {c: _fmt_cell(v, width=200) for c, v in zip(cols, row.values)},
            }
            for row in rows
        ]

    return {
        "db_a": str(report.db_a),
        "db_b": str(report.db_b),
        "identical": report.identical,
        "schema": {
            "tables_only_in_a": list(report.tables_only_in_a),
            "tables_only_in_b": list(report.tables_only_in_b),
            "indexes_only_in_a": list(report.indexes_only_in_a),
            "indexes_only_in_b": list(report.indexes_only_in_b),
            "index_mismatches": [m[0] for m in report.index_mismatches],
            "column_diffs": [
                {
                    "table": cd.table,
                    "only_in_a": list(cd.only_in_a),
                    "only_in_b": list(cd.only_in_b),
                    "mismatched": [m[0] for m in cd.mismatched],
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
                "sample_a_not_b": cells(r.sample_a_not_b, r.columns),
                "sample_b_not_a": cells(r.sample_b_not_a, r.columns),
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
    parser.add_argument(
        "--no-default-ignore",
        action="store_true",
        help="compare every column/row, including import_manifest.import_date",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the report as JSON instead of text",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    ignore: dict[str, TableIgnore] = {} if args.no_default_ignore else DEFAULT_IGNORE
    try:
        report = diff_db_content(
            args.db_a, args.db_b, ignore=ignore, sample_rows=args.sample_rows
        )
    except (FileNotFoundError, sqlite3.Error) as exc:
        print(f"dbdiff: error: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(_report_to_dict(report), indent=2, ensure_ascii=False))
    else:
        print(format_report(report))
    return 0 if report.identical else 1


if __name__ == "__main__":
    raise SystemExit(main())
