"""Source declarations and streaming iteration.

A "source" is a config object describing where to find tables. The
script's ``SOURCES = [...]`` block is built from ``file_source(...)`` and
``sql_source(...)`` calls. At extract time, ``iter_source(src)`` yields
:class:`SourceHandle` objects -- one per table -- carrying a live
connection and a quoted table reference. Classification and summarising
run against that handle without ever materialising rows in Python.

Two source types:

- :class:`FileSource` -- CSVs / delimited text under a directory; the
  iterator registers each file as a DuckDB view in turn.
- :class:`SqlSource` -- tables/views in an ODBC database; the iterator
  holds one pyodbc connection open and yields handles in sequence.

Discovery: a source declared without any filtering info enters discovery
mode. The extract script writes a SOURCES skeleton and exits before
running any aggregation. ``all=True`` opts out of discovery (give me
everything in this source).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .sql_emit import DUCKDB, MSSQL, quote_ident

DEFAULT_FILE_PATTERN = r"\.(csv|txt|tsv)$"


# -- Source dataclasses ---------------------------------------------------


@dataclass
class FileSource:
    path: str
    include: tuple[str, ...] | None = None
    exclude: tuple[str, ...] | None = None
    pattern: str | None = None
    all: bool = False
    encoding: str = "utf-8"
    type: str = field(default="file", init=False)


@dataclass
class SqlSource:
    dsn: str
    tables: Sequence[str] | Mapping[str, str] | None = None
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
) -> FileSource:
    """Declare a file-backed source.

    Discovery triggers when none of ``include``, ``exclude``, ``pattern``,
    or ``all=True`` is supplied.
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
    )


def sql_source(
    dsn: str,
    tables: Sequence[str] | Mapping[str, str] | None = None,
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


# -- Discovery predicate --------------------------------------------------


def needs_discovery(src: Any) -> bool:
    if getattr(src, "all", False):
        return False
    if isinstance(src, FileSource):
        return src.include is None and src.exclude is None and src.pattern is None
    if isinstance(src, SqlSource):
        return src.tables is None and src.pattern is None
    raise TypeError(f"Unknown source: {src!r}")


# -- Handle ---------------------------------------------------------------


@dataclass
class SourceHandle:
    """A single table within a source, ready for ``classify`` / ``summarize``.

    The connection is shared across all handles from one source iteration
    and is closed when the iterator is exhausted (or its caller stops
    consuming and the iterator is garbage-collected).
    """

    dialect: str
    conn: Any
    table: str  # already quoted, ready to drop into ``FROM {table}``
    source_name: str
    source_type: str
    source_detail: dict[str, Any]


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


def iter_file_source(src: FileSource, conn: Any = None) -> Iterator[SourceHandle]:
    """Yield one :class:`SourceHandle` per matched file.

    A DuckDB view is registered for the current file before yielding and
    dropped after the consumer moves on, so peak DuckDB state stays at one
    table even for source directories with hundreds of files.
    """
    import duckdb

    own_conn = conn is None
    if own_conn:
        conn = duckdb.connect()
    files = filter_files(list_files_in_source(src), src)
    _check_unique_basenames(files, src.path)
    try:
        for fp in files:
            view_name = fp.stem
            quoted_view = quote_ident(view_name, DUCKDB)
            quoted_path = str(fp).replace("'", "''")
            conn.execute(
                f"CREATE OR REPLACE VIEW {quoted_view} AS "
                f"SELECT * FROM read_csv_auto('{quoted_path}', header=true)"
            )
            try:
                yield SourceHandle(
                    dialect=DUCKDB,
                    conn=conn,
                    table=quoted_view,
                    source_name=fp.name,
                    source_type="file",
                    source_detail={"path": str(fp)},
                )
            finally:
                conn.execute(f"DROP VIEW IF EXISTS {quoted_view}")
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


def _resolve_table_aliases(
    tables: Sequence[str] | Mapping[str, str],
) -> dict[str, str]:
    if isinstance(tables, Mapping):
        items = list(tables.items())
    else:
        items = [(_strip_schema(t), t) for t in tables]
    seen: dict[str, list[str]] = {}
    for alias, qual in items:
        seen.setdefault(alias, []).append(qual)
    dupes = {a: q for a, q in seen.items() if len(q) > 1}
    if dupes:
        raise ValueError(
            f"Ambiguous table aliases: {sorted(dupes)}. Supply explicit aliases via "
            "a dict, e.g. {'persons_dbo': 'dbo.persons'}."
        )
    return dict(items)


def _quote_qualified(qualified: str) -> str:
    return ".".join(quote_ident(p, MSSQL) for p in qualified.split("."))


def _is_archived(qualified_or_bare: str) -> bool:
    return _strip_schema(qualified_or_bare).lower().startswith("x_")


def _select_sql_tables(conn: Any, src: SqlSource) -> dict[str, str]:
    if src.tables is not None:
        return _resolve_table_aliases(src.tables)
    discovered = list_sql_views(conn, src)
    if src.exclude_archived:
        discovered = [t for t in discovered if not _is_archived(t)]
    if src.pattern is not None:
        pat = re.compile("|".join(src.pattern), re.IGNORECASE)
        discovered = [t for t in discovered if pat.search(t)]
    elif not src.all:
        raise ValueError(
            "sql_source(): provide one of `tables`, `pattern`, or `all=True`."
        )
    return _resolve_table_aliases(discovered)


def iter_sql_source(src: SqlSource, conn: Any = None) -> Iterator[SourceHandle]:
    own_conn = conn is None
    if own_conn:
        conn = sql_connect(src)
    try:
        aliases = _select_sql_tables(conn, src)
        if not aliases:
            raise ValueError(
                f"sql_source(dsn='{src.dsn}'): no tables selected after filters."
            )
        for alias, qual in aliases.items():
            yield SourceHandle(
                dialect=MSSQL,
                conn=conn,
                table=_quote_qualified(qual),
                source_name=alias,
                source_type="sql",
                source_detail={
                    "dsn": src.dsn,
                    "database": src.database,
                    "table": qual,
                },
            )
    finally:
        if own_conn:
            try:
                conn.close()
            except Exception:
                pass


# -- Dispatch -------------------------------------------------------------


def iter_source(src: Any, conn: Any = None) -> Iterator[SourceHandle]:
    if isinstance(src, FileSource):
        return iter_file_source(src, conn=conn)
    if isinstance(src, SqlSource):
        return iter_sql_source(src, conn=conn)
    raise TypeError(f"Unknown source: {src!r}")
