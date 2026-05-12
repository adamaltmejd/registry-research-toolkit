"""Source declarations and streaming iteration.

A "source" is a config object describing where to find tables. The
script's ``configure()`` returns a ``SOURCES`` list built from
``file_source(...)`` and ``sql_source(...)`` calls. At extract time,
``iter_source(src, config=...)`` yields :class:`SourceHandle` objects
-- one per table -- carrying a live connection and a quoted table
reference. Classification and summarising run against that handle
without ever materialising rows in Python.

Two source types:

- :class:`FileSource` -- CSVs / delimited text under a directory; the
  iterator registers each file as a DuckDB view in turn.
- :class:`SqlSource` -- tables/views in an ODBC database; the iterator
  holds one pyodbc connection open and yields handles in sequence.

A SQL source with no ``tables=``, ``pattern=``, or ``all=True`` is
ambiguous in extract mode and ``iter_sql_source`` raises. Discover mode
passes ``permissive=True`` to enumerate everything reachable.

CSV reads are always ``all_varchar=true``: no type sniffer runs, so a
rare row past the default sample window can't crash the read. Extract
mode (``config`` supplied) layers a per-column ``CAST`` on top driven
by ``mock_data_config.json``, then probes any ``opaque`` columns with
``TRY_CAST`` to auto-promote columns that are uniformly numeric or
date into the matching aggregation branch. See
``mock_data_wizard/DESIGN.md`` § *CSV typing*.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Mapping, Sequence, Union

from .sql_emit import DUCKDB, MSSQL, quote_ident

if TYPE_CHECKING:
    from .config import ColumnTypeOverride, MDWConfig

log = logging.getLogger("mdw.sources")

DEFAULT_FILE_PATTERN = r"\.(csv|txt|tsv)$"

# DuckDB read_csv only accepts {utf-8, utf-16, latin-1}. cp1252 is the
# common-on-Windows superset of latin-1; for SCB content (åäö +
# standard punctuation) the latin-1 subset round-trips fine, so accept
# both spellings and forward latin-1 to DuckDB. Anything outside the
# accepted set is forwarded verbatim and DuckDB will reject it with its
# own error.
_DUCKDB_ENCODINGS = {"utf-8", "utf-16", "latin-1"}
_ENCODING_ALIASES = {
    "utf8": "utf-8",
    "utf_8": "utf-8",
    "utf16": "utf-16",
    "utf_16": "utf-16",
    "latin1": "latin-1",
    "latin_1": "latin-1",
    "iso-8859-1": "latin-1",
    "iso_8859_1": "latin-1",
    "cp1252": "latin-1",
    "windows-1252": "latin-1",
    "windows_1252": "latin-1",
}


def _normalise_csv_encoding(encoding: str) -> str:
    key = encoding.strip().lower()
    return _ENCODING_ALIASES.get(key, key)


# Files at or below this size are loaded into a DuckDB TABLE instead of
# registered as a VIEW. The TABLE materialises read_csv_auto once,
# avoiding per-aggregate-query reparses; VIEW keeps memory bounded for
# files that would not fit. Override via MDW_MEMORY_THRESHOLD_MB.
#
# Default: 50 GiB. The MONA batch server has 150-200 GB RAM and DuckDB
# defaults to ~80% of RAM as its budget; sources iterate sequentially
# and DROP TABLE between handles, so peak DuckDB memory is bounded to
# one source at a time (PERCENTILE_CONT sorts spill to TEMP if needed).
# CI / laptop runs use small fixtures so the threshold is irrelevant
# there; lower it via the env var if a host is tighter on RAM.
_DEFAULT_MEMORY_THRESHOLD_MB = 50 * 1024


# -- Source dataclasses ---------------------------------------------------


@dataclass
class FileSource:
    path: str
    include: tuple[str, ...] | None = None
    exclude: tuple[str, ...] | None = None
    pattern: str | None = None
    all: bool = False
    encoding: str = "utf-8"
    where: str | None = None
    type: str = field(default="file", init=False)


@dataclass(frozen=True)
class SqlTable:
    """A single SQL table reference with optional cohort filter and alias.

    ``qualified`` is the dialect-specific table identifier (``schema.name``
    on MS SQL). ``where`` becomes a ``WHERE`` clause applied server-side
    before any aggregation, scoped to this table only. ``alias`` defaults
    to the unqualified portion of ``qualified`` and is used as the
    ``source_name`` in ``mock_data_stats.json`` and as the dedup key when multiple
    schemas share a table name.
    """

    qualified: str
    where: str | None = None
    alias: str | None = None


# What sql_source(tables=) accepts. A bare string is shorthand for
# SqlTable(qualified=...). Mapping keys override the SqlTable.alias.
SqlTableSpec = Union[str, SqlTable]


@dataclass
class SqlSource:
    dsn: str
    tables: Sequence[SqlTableSpec] | Mapping[str, SqlTableSpec] | None = None
    pattern: tuple[str, ...] | None = None
    schema: tuple[str, ...] | None = None
    driver: str | None = None
    server: str | None = None
    database: str | None = None
    all: bool = False
    exclude_archived: bool = True
    type: str = field(default="sql", init=False)


# -- Constructors ---------------------------------------------------------


def file_source(
    path: str,
    include: Sequence[str] | None = None,
    exclude: Sequence[str] | None = None,
    pattern: str | None = None,
    all: bool = False,
    encoding: str = "utf-8",
    where: str | None = None,
) -> FileSource:
    """Declare a file-backed source.

    Discovery triggers when none of ``include``, ``exclude``, ``pattern``,
    or ``all=True`` is supplied.

    ``where``: SQL predicate applied as ``WHERE {where}`` to every file in
    the source before aggregation. Useful for multi-year CSVs that should
    be narrowed to a cohort. The clause runs against the DuckDB-typed
    columns from ``read_csv_auto``.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("file_source(): `path` must be a non-empty string")
    return FileSource(
        path=path,
        include=tuple(include) if include is not None else None,
        exclude=tuple(exclude) if exclude is not None else None,
        pattern=pattern,
        all=all,
        encoding=encoding,
        where=where,
    )


def sql_table(
    qualified: str,
    where: str | None = None,
    alias: str | None = None,
) -> SqlTable:
    """Declare a single SQL table with an optional WHERE filter.

    ``where`` is applied server-side as a derived-table predicate,
    scoped to this table alone. Use this when different tables in the
    same source need different cohort filters -- a source-wide
    ``where=`` would apply to columns the table may not have.

        sql_source(
            dsn="P1105",
            tables=(
                sql_table("dbo.lisa_2018", where="AR > 2015"),
                sql_table("dbo.par",       where="INDATUM > '2015-01-01'"),
                "dbo.fodelse",  # plain string -> no filter
            ),
        )
    """
    if not isinstance(qualified, str) or not qualified:
        raise ValueError("sql_table(): `qualified` must be a non-empty string")
    return SqlTable(qualified=qualified, where=where, alias=alias)


def sql_source(
    dsn: str,
    tables: Sequence[SqlTableSpec] | Mapping[str, SqlTableSpec] | None = None,
    pattern: str | Sequence[str] | None = None,
    schema: str | Sequence[str] | None = None,
    driver: str | None = None,
    server: str | None = None,
    database: str | None = None,
    all: bool = False,
    exclude_archived: bool = True,
) -> SqlSource:
    """Declare an ODBC-backed source.

    Discovery triggers when none of ``tables``, ``pattern``, or
    ``all=True`` is supplied.

    Per-table filters live on individual ``sql_table()`` entries; there
    is no source-wide ``where`` because heterogeneous tables in one
    source typically need different (or no) predicates.
    """
    if not isinstance(dsn, str) or not dsn:
        raise ValueError("sql_source(): `dsn` must be a non-empty string")
    if isinstance(pattern, str):
        pattern = (pattern,)
    elif pattern is not None:
        pattern = tuple(pattern)
    if isinstance(schema, str):
        schema = (schema,)
    elif schema is not None:
        schema = tuple(schema)
    return SqlSource(
        dsn=dsn,
        tables=tables,
        pattern=pattern,
        schema=schema,
        driver=driver,
        server=server,
        database=database,
        all=all,
        exclude_archived=exclude_archived,
    )


# -- Handle ---------------------------------------------------------------


@dataclass
class SourceHandle:
    """A single table within a source, ready for ``classify`` / ``summarize``.

    ``table`` is whatever the caller pastes into ``FROM {table}``: a
    quoted view/table name, or, when the source has a ``where`` filter,
    a quoted derived-table expression like ``(SELECT * FROM ... WHERE
    ...) AS __mdw_src``. Downstream emitters don't care which.

    The connection is shared across all handles from one source iteration
    and is closed when the iterator is exhausted (or its caller stops
    consuming and the iterator is garbage-collected).
    """

    dialect: str
    conn: Any
    table: str
    source_name: str
    source_type: str
    source_detail: dict[str, Any]


# -- WHERE-clause routing --------------------------------------------------


_DERIVED_ALIAS = "__mdw_src"


def _wrap_with_where(table_ref: str, where: str | None) -> str:
    """Wrap a quoted table reference in a derived-table that applies WHERE.

    Pasting the result into ``FROM {table_ref}`` runs the predicate
    server-side before any aggregation -- transparent to every emitter.
    """
    if not where:
        return table_ref
    return f"(SELECT * FROM {table_ref} WHERE {where}) AS {_DERIVED_ALIAS}"


# -- File iteration -------------------------------------------------------


def list_files_in_source(src: FileSource) -> list[Path]:
    p = Path(src.path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"file_source path not found: {src.path}")
    if p.is_file():
        return [p]
    pat = re.compile(src.pattern or DEFAULT_FILE_PATTERN, re.IGNORECASE)
    found = [q for q in p.rglob("*") if q.is_file() and pat.search(q.name)]
    return sorted(found)


def filter_files(found: Sequence[Path], src: FileSource) -> list[Path]:
    out = list(found)
    if src.include is not None:
        inc = set(src.include)
        out = [f for f in out if f.name in inc]
    if src.exclude is not None:
        exc = set(src.exclude)
        out = [f for f in out if f.name not in exc]
    return out


def _check_unique_basenames(files: Sequence[Path], src_path: str) -> None:
    seen: dict[str, list[Path]] = {}
    for f in files:
        seen.setdefault(f.name, []).append(f)
    dupes = {n: ps for n, ps in seen.items() if len(ps) > 1}
    if dupes:
        msgs = "; ".join(f"{n} -> {[str(p) for p in ps]}" for n, ps in dupes.items())
        raise ValueError(
            f"Duplicate file basename(s) in source '{src_path}': {msgs}. "
            "Narrow `path =` to a subdirectory to select a specific file."
        )


def _semantic_to_duckdb_cast(override: ColumnTypeOverride) -> str | None:
    """DuckDB type to CAST a CSV-as-VARCHAR column into for aggregation.

    Returns ``None`` for types that work fine as VARCHAR (id,
    categorical, opaque, date) — the column is left untyped and the
    aggregation queries operate on strings:

    - ``id``: opaque token. SCB pids and study-IDs commonly carry leading
      zeros that a numeric cast would silently drop.
    - ``categorical``: code lists like ``"01"`` / ``"1"`` that should not
      collapse under a numeric cast.
    - ``opaque``: free text; only length stats are computed.
    - ``date``: SCB date formats are a zoo (YYYYMMDD, DD/MM/YYYY, …) and
      parsing happens in :func:`summarize._to_date`. Lexicographic
      ``MIN``/``MAX`` is correct for the SCB-common ISO and YYYYMMDD
      shapes; pinning a ``date_format`` in the override does not change
      the SQL path today.

    Only ``numeric`` requires a cast for ``MIN``/``MAX``/``AVG`` /
    ``STDDEV`` / ``PERCENTILE_CONT`` to behave as numbers.
    ``numeric_subtype="integer"`` → ``BIGINT`` (clean integer round-trip
    on min/max). Otherwise ``DOUBLE`` (covers floats and integers up to
    2^53).
    """
    if override.type == "numeric":
        return "BIGINT" if override.numeric_subtype == "integer" else "DOUBLE"
    return None


def _build_varchar_select(quoted_path: str, encoding: str) -> str:
    """SELECT that reads the CSV as all-VARCHAR.

    ``all_varchar=true`` skips DuckDB's type sniffer entirely — every
    column comes back as VARCHAR and ``nullstr=['', ' ']`` still
    applies, so the SCB ' ' sentinel becomes NULL. No inference means
    no rare-row crash from a sample that disagreed with later rows.
    """
    quoted_encoding = encoding.replace("'", "''")
    return (
        f"SELECT * FROM read_csv_auto("
        f"'{quoted_path}', header=true, encoding='{quoted_encoding}', "
        f"nullstr=['', ' '], all_varchar=true)"
    )


def _build_cast_select(
    conn: Any,
    quoted_path: str,
    encoding: str,
    overrides: Mapping[str, ColumnTypeOverride],
) -> str:
    """Build a SELECT that reads the CSV as VARCHAR and casts per-column.

    DESCRIBE on the inner all-varchar SELECT pulls the column list from
    the header without scanning the file. Columns absent from
    ``overrides`` pass through as VARCHAR; ``extract.process_handle``
    validates the full column set against the config and raises one
    error listing every missing override.
    """
    raw_select = _build_varchar_select(quoted_path, encoding)
    desc = conn.execute(f"DESCRIBE {raw_select}").fetchall()
    col_names = [r[0] for r in desc]
    parts: list[str] = []
    for name in col_names:
        qname = quote_ident(name, DUCKDB)
        ov = overrides.get(name)
        cast_type = _semantic_to_duckdb_cast(ov) if ov is not None else None
        if cast_type is None:
            parts.append(qname)
        else:
            parts.append(f"CAST({qname} AS {cast_type}) AS {qname}")
    # The derived-table alias has its own namespace from column names in
    # DuckDB, so a column named e.g. `_raw` wouldn't collide; still, use
    # a distinctly internal name that no SCB CSV header is likely to use.
    return f"SELECT {', '.join(parts)} FROM ({raw_select}) AS __mdw_csv_inner"


def _probe_and_promote_opaque(
    conn: Any,
    quoted_table: str,
    overrides: dict[str, ColumnTypeOverride],
    *,
    file_name: str,
) -> list[str]:
    """Auto-promote opaque columns to numeric/date when TRY_CAST is clean.

    For each column the user marked ``opaque``, batched into one query:
    count non-null rows, then count how many cleanly ``TRY_CAST`` to
    ``BIGINT``, ``DOUBLE``, ``DATE``. When every non-null value casts
    to the same target, the override is mutated in place to that type.

    Why this is safe to do without explicit user opt-in: the numeric
    and date branches emit only **perturbed aggregates** (min/max with
    relative noise, mean/sd, date min/max with ±N-day jitter). No
    individual values land in the output. Promotion to ``categorical``
    or ``id`` is deliberately NOT supported here — those branches emit
    frequency tables and distinct-value lists, which would leak PII if
    we promoted a misclassified opaque column. ``opaque`` stays as-is
    when none of the cast targets is clean.

    Each promotion is logged at WARNING so the MONA-side run log
    records the decision and the user can override in the next config
    iteration.

    Returns the list of promoted column names (caller order). ``overrides``
    is mutated in place; ``MDWConfig.column_types`` is the same dict
    object the caller looked up, so ``extract.process_handle`` sees the
    promoted type without an extra plumbing step.

    Caveat: DuckDB's ``TRY_CAST(... AS DATE)`` accepts ISO 'YYYY-MM-DD'
    and similar. YYYYMMDD strings (common in SCB) satisfy BIGINT first
    and promote to ``numeric/integer``. The warning makes that visible;
    the user can flip the override to ``date`` in the next iteration.
    """
    opaque_cols = [c for c, ov in overrides.items() if ov.type == "opaque"]
    if not opaque_cols:
        return []
    # The BIGINT predicate is a round-trip check, not a bare TRY_CAST:
    # DuckDB happily rounds "1.5" -> 2 when casting VARCHAR to BIGINT,
    # so the naive ``TRY_CAST(... AS BIGINT) IS NOT NULL`` matches
    # float-shaped values. Requiring the BIGINT result to equal the
    # DOUBLE result keeps "1", "1e3", "3.0" classified as integer
    # while pushing "1.5", "2.5" down to DOUBLE.
    parts: list[str] = []
    for i, col in enumerate(opaque_cols):
        qcol = quote_ident(col, DUCKDB)
        parts.extend(
            [
                f"COUNT(*) FILTER (WHERE {qcol} IS NOT NULL) AS c{i}_nn",
                f"COUNT(*) FILTER (WHERE {qcol} IS NOT NULL "
                f"AND TRY_CAST({qcol} AS BIGINT) IS NOT NULL "
                f"AND TRY_CAST({qcol} AS BIGINT) = TRY_CAST({qcol} AS DOUBLE)) "
                f"AS c{i}_bi",
                f"COUNT(*) FILTER (WHERE {qcol} IS NOT NULL "
                f"AND TRY_CAST({qcol} AS DOUBLE) IS NOT NULL) AS c{i}_db",
                f"COUNT(*) FILTER (WHERE {qcol} IS NOT NULL "
                f"AND TRY_CAST({qcol} AS DATE) IS NOT NULL) AS c{i}_dt",
            ]
        )
    sql = f"SELECT {', '.join(parts)} FROM {quoted_table}"
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
    finally:
        cur.close()
    promoted: list[str] = []
    for i, col in enumerate(opaque_cols):
        nn = row[4 * i]
        bi = row[4 * i + 1]
        db = row[4 * i + 2]
        dt = row[4 * i + 3]
        if nn == 0:
            continue
        if bi == nn:
            new_type, new_sub = "numeric", "integer"
        elif db == nn:
            new_type, new_sub = "numeric", "double"
        elif dt == nn:
            new_type, new_sub = "date", None
        else:
            continue
        overrides[col] = replace(
            overrides[col],
            type=new_type,
            numeric_subtype=new_sub if new_type == "numeric" else None,
        )
        log.warning(
            "[%s] column %r auto-promoted opaque -> %s%s "
            "(every non-null value TRY_CASTs cleanly). Override in "
            "mock_data_config.json if wrong.",
            file_name,
            col,
            new_type,
            f"/{new_sub}" if new_sub else "",
        )
        promoted.append(col)
    return promoted


def iter_file_source(
    src: FileSource,
    conn: Any = None,
    *,
    config: MDWConfig | None = None,
) -> Iterator[SourceHandle]:
    """Yield one :class:`SourceHandle` per matched file.

    A DuckDB view is registered for the current file before yielding and
    dropped after the consumer moves on, so peak DuckDB state stays at one
    table even for source directories with hundreds of files.

    Reads are always ``all_varchar=true`` — no type sniffer, so a rare
    row past the default sample window can't crash the load. With
    ``config`` supplied (extract mode), per-column ``CAST``s from
    ``config.column_types`` are layered on top and any ``opaque``
    columns are probed via ``TRY_CAST``; columns whose every non-null
    value is uniformly numeric or date are auto-promoted in place (and
    a WARNING is logged per promotion). The promotion is applied via
    ``ALTER COLUMN TYPE`` on the TABLE path so the file is read exactly
    once. Without ``config`` (discover mode, or a file not present in
    the config) every column reports ``VARCHAR``; the classifier
    downstream relies on name patterns and regmeta evidence rather than
    a SQL-type signal.
    """
    import duckdb

    own_conn = conn is None
    if own_conn:
        conn = duckdb.connect()
    files = filter_files(list_files_in_source(src), src)
    _check_unique_basenames(files, src.path)
    threshold_bytes = (
        int(os.environ.get("MDW_MEMORY_THRESHOLD_MB", _DEFAULT_MEMORY_THRESHOLD_MB))
        * 1024
        * 1024
    )
    encoding = _normalise_csv_encoding(src.encoding)
    try:
        for fp in files:
            view_name = fp.stem
            quoted_view = quote_ident(view_name, DUCKDB)
            quoted_path = str(fp).replace("'", "''")
            materialise = fp.stat().st_size <= threshold_bytes
            kind = "TABLE" if materialise else "VIEW"
            overrides: dict[str, ColumnTypeOverride] = (
                config.column_types.get(fp.name, {}) if config is not None else {}
            )
            log.info(
                "[%s] read_csv (%s, encoding=%s, %.1f MB, %s)",
                fp.name,
                kind.lower(),
                encoding,
                fp.stat().st_size / (1024 * 1024),
                "config-cast" if overrides else "all-varchar",
            )
            try:
                if overrides:
                    select_sql = _build_cast_select(
                        conn, quoted_path, encoding, overrides
                    )
                else:
                    select_sql = _build_varchar_select(quoted_path, encoding)
                conn.execute(f"CREATE OR REPLACE {kind} {quoted_view} AS {select_sql}")
                # Extract-mode auto-promotion: probe opaque columns and
                # apply any flips. On the TABLE path we ALTER COLUMN TYPE
                # in place so the file is read exactly once (the cast
                # runs against the already-materialised VARCHAR data).
                # On the VIEW path the materialisation didn't actually
                # read the file, so we rebuild the view SQL — the next
                # downstream query reads with the final casts in one pass.
                if overrides:
                    promoted = _probe_and_promote_opaque(
                        conn, quoted_view, overrides, file_name=fp.name
                    )
                    if promoted and kind == "TABLE":
                        for col in promoted:
                            cast_type = _semantic_to_duckdb_cast(overrides[col])
                            if cast_type is None:
                                continue  # date promotion stays VARCHAR
                            qcol = quote_ident(col, DUCKDB)
                            conn.execute(
                                f"ALTER TABLE {quoted_view} ALTER COLUMN {qcol} "
                                f"TYPE {cast_type} USING CAST({qcol} AS {cast_type})"
                            )
                    elif promoted:
                        select_sql = _build_cast_select(
                            conn, quoted_path, encoding, overrides
                        )
                        conn.execute(
                            f"CREATE OR REPLACE {kind} {quoted_view} AS {select_sql}"
                        )
            except Exception as exc:
                hint = (
                    " Try `file_source(..., encoding='latin-1')` if the file"
                    " is Windows-1252 (common for SCB exports)."
                    if encoding == "utf-8"
                    else ""
                )
                raise RuntimeError(
                    f"DuckDB rejected {fp.name} with encoding={encoding!r}: "
                    f"{exc}.{hint}"
                ) from exc
            where = src.where
            table_ref = _wrap_with_where(quoted_view, where)
            detail: dict[str, Any] = {"path": str(fp)}
            if where:
                detail["where"] = where
            try:
                yield SourceHandle(
                    dialect=DUCKDB,
                    conn=conn,
                    table=table_ref,
                    source_name=fp.name,
                    source_type="file",
                    source_detail=detail,
                )
            finally:
                conn.execute(f"DROP {kind} IF EXISTS {quoted_view}")
    finally:
        if own_conn:
            conn.close()


# -- SQL iteration --------------------------------------------------------


def _build_pyodbc_connstr(src: SqlSource) -> str:
    parts = [f"DSN={src.dsn}"]
    if src.driver:
        parts.append(f"Driver={{{src.driver}}}")
    if src.server:
        parts.append(f"Server={src.server}")
    if src.database:
        parts.append(f"Database={src.database}")
    parts.append("Trusted_Connection=yes")
    return ";".join(parts)


def sql_connect(src: SqlSource) -> Any:
    import pyodbc

    return pyodbc.connect(_build_pyodbc_connstr(src))


def list_sql_views(conn: Any, src: SqlSource) -> list[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables "
            "WHERE TABLE_TYPE = 'VIEW'"
        )
        out: list[str] = []
        for s, t in cur.fetchall():
            if src.schema is not None and s not in src.schema:
                continue
            out.append(f"{s}.{t}")
        return sorted(set(out))
    finally:
        cur.close()


def _strip_schema(qual: str) -> str:
    return qual.rsplit(".", 1)[-1]


def _normalize_to_sql_tables(
    tables: Sequence[SqlTableSpec] | Mapping[str, SqlTableSpec],
) -> list[SqlTable]:
    """Coerce strings, SqlTable, and alias-maps into a uniform list."""
    out: list[SqlTable] = []
    if isinstance(tables, Mapping):
        for alias, val in tables.items():
            if isinstance(val, str):
                out.append(SqlTable(qualified=val, alias=alias))
            elif isinstance(val, SqlTable):
                # Mapping key wins as the alias if the SqlTable didn't set one.
                if val.alias is None:
                    out.append(replace(val, alias=alias))
                else:
                    out.append(val)
            else:
                raise TypeError(
                    f"tables[{alias!r}] must be str or SqlTable; "
                    f"got {type(val).__name__}"
                )
        return out
    for t in tables:
        if isinstance(t, str):
            out.append(SqlTable(qualified=t))
        elif isinstance(t, SqlTable):
            out.append(t)
        else:
            raise TypeError(
                f"tables entries must be str or SqlTable; got {type(t).__name__}"
            )
    return out


def _resolve_sql_aliases(
    normalized: Sequence[SqlTable], *, on_collision: str = "raise"
) -> dict[str, SqlTable]:
    """Map alias -> SqlTable.

    ``on_collision`` controls behavior when two tables share an alias:

    - ``"raise"`` (default, extract mode): raise ``ValueError``. The
      user explicitly named these tables; ambiguity is a config bug.
    - ``"qualify"`` (discover mode): the user didn't pick the table
      list -- the iterator did -- so we silently disambiguate by
      keying both colliding rows under their qualified names.
    """
    seen: dict[str, list[SqlTable]] = {}
    for t in normalized:
        alias = t.alias or _strip_schema(t.qualified)
        seen.setdefault(alias, []).append(t)
    dupes = {a: ts for a, ts in seen.items() if len(ts) > 1}
    if dupes and on_collision == "raise":
        names = sorted(dupes)
        raise ValueError(
            f"Ambiguous table aliases: {names}. Pass explicit alias= on "
            f"sql_table(), e.g. sql_table('dbo.persons', alias='persons_dbo')."
        )
    out: dict[str, SqlTable] = {}
    for alias, group in seen.items():
        if len(group) == 1:
            out[alias] = group[0]
        else:
            for t in group:
                out[t.qualified] = t
    return out


def _quote_qualified(qualified: str) -> str:
    return ".".join(quote_ident(p, MSSQL) for p in qualified.split("."))


def _is_archived(qualified_or_bare: str) -> bool:
    return _strip_schema(qualified_or_bare).lower().startswith("x_")


def _select_sql_tables(
    conn: Any, src: SqlSource, *, permissive: bool = False
) -> dict[str, SqlTable]:
    """Resolve which tables a SQL source iterates.

    ``permissive=True`` (discover mode) treats absent ``tables``/
    ``pattern``/``all`` as "give me everything reachable". Extract mode
    keeps the strict default so a typo'd source can't silently match
    every view in the DSN.
    """
    if src.tables is not None:
        return _resolve_sql_aliases(_normalize_to_sql_tables(src.tables))
    discovered = list_sql_views(conn, src)
    if src.exclude_archived:
        discovered = [t for t in discovered if not _is_archived(t)]
    if src.pattern is not None:
        pat = re.compile("|".join(src.pattern), re.IGNORECASE)
        discovered = [t for t in discovered if pat.search(t)]
    elif not src.all and not permissive:
        raise ValueError(
            "sql_source(): provide one of `tables`, `pattern`, or `all=True`."
        )
    return _resolve_sql_aliases(
        [SqlTable(qualified=t) for t in discovered],
        on_collision="qualify" if permissive else "raise",
    )


def iter_sql_source(
    src: SqlSource, conn: Any = None, *, permissive: bool = False
) -> Iterator[SourceHandle]:
    own_conn = conn is None
    if own_conn:
        conn = sql_connect(src)
    try:
        aliases = _select_sql_tables(conn, src, permissive=permissive)
        if not aliases:
            raise ValueError(
                f"sql_source(dsn='{src.dsn}'): no tables selected after filters."
            )
        for alias, sqltbl in aliases.items():
            table_ref = _wrap_with_where(
                _quote_qualified(sqltbl.qualified), sqltbl.where
            )
            detail: dict[str, Any] = {
                "dsn": src.dsn,
                "database": src.database,
                "table": sqltbl.qualified,
            }
            if sqltbl.where:
                detail["where"] = sqltbl.where
            yield SourceHandle(
                dialect=MSSQL,
                conn=conn,
                table=table_ref,
                source_name=alias,
                source_type="sql",
                source_detail=detail,
            )
    finally:
        if own_conn:
            try:
                conn.close()
            except Exception:
                pass


# -- Dispatch -------------------------------------------------------------


def iter_source(
    src: Any,
    conn: Any = None,
    *,
    permissive: bool = False,
    config: MDWConfig | None = None,
) -> Iterator[SourceHandle]:
    """Dispatch ``src`` to the file/SQL iterator.

    ``config`` is forwarded to file iteration to drive the
    ``all_varchar=true`` + per-column CAST path. SQL sources read
    properly-typed columns from the server and ignore the config here.
    ``permissive`` is the discover-mode flag for SQL listing and is
    irrelevant to file sources.
    """
    if isinstance(src, FileSource):
        return iter_file_source(src, conn=conn, config=config)
    if isinstance(src, SqlSource):
        return iter_sql_source(src, conn=conn, permissive=permissive)
    raise TypeError(f"Unknown source: {src!r}")
