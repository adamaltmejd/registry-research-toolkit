"""Entry points for the on-MONA discover and extract steps.

Two modes, one bundle:

- ``MODE = "discover"`` -- metadata-only walk over ``SOURCES``. SQL
  sources read ``INFORMATION_SCHEMA.COLUMNS`` and ``COUNT(*)``; file
  sources use DuckDB ``DESCRIBE`` and ``COUNT(*)``. No samples, no
  distinct counts. Output: ``discover.json``. The user copies it
  off MONA and runs ``mock-data-wizard configure`` locally to author
  ``mdw_config.json``.
- ``MODE = "extract"`` -- typed aggregation. Reads ``mdw_config.json``
  uploaded next to the bundle, requires every column to have a type
  override, and produces ``stats.json``. No data-driven classifier
  pass: the configured type drives the per-column SQL.

PII discipline: only aggregate values cross the JSON boundary. Cell
suppression and noise live in :mod:`summarize`; this module just
orchestrates and routes the export through :func:`scan.write_export`.
"""

from __future__ import annotations

import logging
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from .classify import classify_column
from .config import MDWConfig, load_config
from .scan import write_export
from .sources import SourceHandle, iter_source
from .sql_emit import DUCKDB, MSSQL, quote_ident
from .summarize import small_pop_threshold, summarize_column

log = logging.getLogger("mdw.extract")

CONTRACT_VERSION = "2.0.0"
DISCOVER_CONTRACT_VERSION = "discover-1.0.0"
SAMPLE_SIZE = 1000


# -- Per-table SQL helpers -------------------------------------------------


def _count_rows(conn: Any, table: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        (n,) = cur.fetchone()
        return int(n)
    finally:
        cur.close()


def _list_columns(conn: Any, table: str, dialect: str) -> list[str]:
    sql = (
        f"SELECT TOP 0 * FROM {table}"
        if dialect == MSSQL
        else f"SELECT * FROM {table} LIMIT 0"
    )
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return [d[0] for d in cur.description]
    finally:
        cur.close()


def _describe_columns_sql(conn: Any, qualified: str) -> list[dict[str, Any]]:
    """Pull column metadata for a SQL table from INFORMATION_SCHEMA.

    ``qualified`` is ``schema.name`` (unquoted). The query targets the
    server's catalog, not the table itself, so it works against views
    we don't have row-level access to.
    """
    schema, _, name = qualified.partition(".")
    if not name:
        # Bare table name (no schema) -- match anywhere.
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = ? "
                "ORDER BY ORDINAL_POSITION",
                (qualified,),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
    else:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                "ORDER BY ORDINAL_POSITION",
                (schema, name),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
    return [
        {"name": r[0], "sql_type": r[1], "nullable": str(r[2]).upper() == "YES"}
        for r in rows
    ]


def _describe_columns_duckdb(conn: Any, table: str) -> list[dict[str, Any]]:
    """Pull column metadata from DuckDB ``DESCRIBE``.

    Works on any FROM-able expression (registered view, registered table,
    or a ``read_csv_auto(...)`` call). DESCRIBE returns
    ``column_name, column_type, null, key, default, extra``.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"DESCRIBE SELECT * FROM {table} LIMIT 0")
        rows = cur.fetchall()
    finally:
        cur.close()
    return [
        {"name": r[0], "sql_type": r[1], "nullable": str(r[2]).upper() == "YES"}
        for r in rows
    ]


def _count_distinct_and_nulls(
    conn: Any, table: str, col: str, dialect: str
) -> tuple[int, int]:
    """Return ``(n_distinct, null_count)`` for one column.

    Always called -- both counts land in ``stats.json`` and are not
    derivable from a type override.
    """
    qcol = quote_ident(col, dialect)
    sql = (
        f"SELECT COUNT(DISTINCT {qcol}) AS n_distinct, "
        f"SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END) AS null_count "
        f"FROM {table}"
    )
    cur = conn.cursor()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return int(row[0] or 0), int(row[1] or 0)
    finally:
        cur.close()


def _sample_values(
    conn: Any,
    table: str,
    col: str,
    dialect: str,
    sample_n: int = SAMPLE_SIZE,
    seed: int = 0,
) -> list[Any]:
    """Return a deterministic sample of non-null values for one column.

    Same data + same seed yields the same sample. DuckDB uses a seeded
    reservoir sample; MSSQL orders by a content hash of the column
    value so sibling tables with the same shape sample the same rows
    (the seed has no effect on the MSSQL branch -- the hash is the
    determinism source).
    """
    qcol = quote_ident(col, dialect)
    if dialect == DUCKDB:
        # `reservoir` is required: bare `USING SAMPLE N ROWS REPEATABLE (s)`
        # errors out in current DuckDB.
        sample_sql = (
            f"SELECT {qcol} FROM {table} WHERE {qcol} IS NOT NULL "
            f"USING SAMPLE reservoir({sample_n} ROWS) REPEATABLE ({seed})"
        )
    else:
        # SQL Server's TOP N without ORDER BY returns scan-order rows --
        # stable within a session on a heap, but not across reruns or
        # across same-shape sibling tables.
        #
        # Ties on this ORDER BY key (rows that share the column value all
        # hash to the same bucket) are deliberately not broken: the
        # sample is consumed for type classification, where what matters
        # is which *values* appear, not which physical rows. The set of
        # values whose hash sorts before the TOP cut is fully
        # deterministic; only the last value's row count can wobble
        # within its single bucket, and a row-level tiebreaker would
        # require a universal row id we don't have.
        sample_sql = (
            f"SELECT TOP {sample_n} {qcol} FROM {table} "
            f"WHERE {qcol} IS NOT NULL "
            f"ORDER BY HASHBYTES('SHA1', CAST({qcol} AS NVARCHAR(MAX)))"
        )
    cur = conn.cursor()
    try:
        cur.execute(sample_sql)
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()


# -- Per-handle pipeline (extract mode) -----------------------------------


def process_handle(
    handle: SourceHandle,
    rng: random.Random,
    *,
    classifier_seed: int = 0,
    config: MDWConfig | None = None,
    require_typed: bool = False,
) -> dict[str, Any]:
    """Process one :class:`SourceHandle` into a source-level stats dict.

    ``require_typed=True`` (extract mode) errors when a column has no
    override in ``config`` -- the classifier path is gone in that mode.
    ``False`` (legacy / fallback) lets ``classify_column`` decide.
    """
    log.info("[%s] counting rows...", handle.source_name)
    _flush_log_handlers()
    t0 = time.monotonic()
    n_rows = _count_rows(handle.conn, handle.table)
    log.info(
        "[%s] %d rows (%.1fs)",
        handle.source_name,
        n_rows,
        time.monotonic() - t0,
    )
    _flush_log_handlers()
    if n_rows < small_pop_threshold():
        log.warning(
            "source %r has only %d rows (< %d). Aggregates may be "
            "identifiable even after k-anonymity.",
            handle.source_name,
            n_rows,
            small_pop_threshold(),
        )

    cols = _list_columns(handle.conn, handle.table, handle.dialect)
    log.info("[%s] %d columns to summarise", handle.source_name, len(cols))
    _flush_log_handlers()

    columns_out: list[dict[str, Any]] = []
    for i, col in enumerate(cols, 1):
        t_col = time.monotonic()
        n_distinct, null_count = _count_distinct_and_nulls(
            handle.conn, handle.table, col, handle.dialect
        )

        override = config.lookup_type(handle.source_name, col) if config else None
        if require_typed and override is None:
            raise RuntimeError(
                f"extract mode: column {handle.source_name!r}.{col!r} has no "
                f"type override in mdw_config.json. Re-run discover and "
                f"configure to refresh it, or add an entry by hand."
            )
        options = config.lookup_options(handle.source_name, col) if config else {}
        # Skip the per-column sample query when an inline hint pins the
        # subtype/format -- nothing downstream consumes the sample then.
        sample: list[Any] = (
            []
            if override is not None and override.has_inline_hint()
            else _sample_values(
                handle.conn,
                handle.table,
                col,
                handle.dialect,
                seed=classifier_seed,
            )
        )
        col_type = (
            override.type
            if override is not None
            else classify_column(col, n_rows, n_distinct, sample)
        )

        columns_out.append(
            summarize_column(
                handle.conn,
                handle.table,
                col,
                col_type,
                n_rows=n_rows,
                n_distinct=n_distinct,
                null_count=null_count,
                sample=sample,
                dialect=handle.dialect,
                rng=rng,
                override=override,
                options=options,
            )
        )
        log.debug(
            "[%s] col %d/%d %s -> %s (%s) (%.1fs)",
            handle.source_name,
            i,
            len(cols),
            col,
            col_type,
            "override" if override else "auto",
            time.monotonic() - t_col,
        )
        _flush_log_handlers()

    return {
        "source_name": handle.source_name,
        "source_type": handle.source_type,
        "source_detail": handle.source_detail,
        "row_count": n_rows,
        "columns": columns_out,
    }


def _flush_log_handlers() -> None:
    for h in logging.getLogger().handlers:
        try:
            h.flush()
        except Exception:
            pass


def _shared_columns(source_results: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Columns that appear in 2+ sources, with the max n_distinct seen."""
    seen: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"sources": [], "max_n_distinct": 0}
    )
    for src in source_results:
        for col in src["columns"]:
            entry = seen[col["column_name"]]
            entry["sources"].append(src["source_name"])
            entry["max_n_distinct"] = max(
                entry["max_n_distinct"], int(col["n_distinct"])
            )
    return [
        {
            "column_name": cname,
            "sources": sorted(set(e["sources"])),
            "max_n_distinct": e["max_n_distinct"],
        }
        for cname, e in sorted(seen.items())
        if len(set(e["sources"])) >= 2
    ]


# -- Top-level orchestration: extract mode --------------------------------


def run_extract_typed(
    sources: Iterable[Any],
    output_path: Path,
    config: MDWConfig,
    *,
    seed: int | None = None,
    classifier_seed: int = 0,
) -> dict[str, Any]:
    """Run the typed extract pipeline and write ``stats.json``.

    Every column must carry a type override in ``config`` -- this mode
    has no classifier fallback. Run ``discover`` + ``configure`` first
    to author the file. ``seed`` controls Python-side noise injection;
    ``classifier_seed`` controls the per-column sample for subtype /
    date-format detection (still used when the override is present
    without an inline hint).
    """
    rng = random.Random(seed)
    sources = list(sources)
    log.info("run_extract_typed: %d source declaration(s)", len(sources))
    _flush_log_handlers()
    source_results: list[dict[str, Any]] = []
    for src_idx, src in enumerate(sources, 1):
        log.info("source %d/%d: %r", src_idx, len(sources), src)
        _flush_log_handlers()
        for handle in iter_source(src):
            source_results.append(
                process_handle(
                    handle,
                    rng,
                    classifier_seed=classifier_seed,
                    config=config,
                    require_typed=True,
                )
            )
            log.info(
                "source %d/%d: handle done (%d total handle(s) so far)",
                src_idx,
                len(sources),
                len(source_results),
            )
            _flush_log_handlers()

    if not source_results:
        raise RuntimeError(
            "No data sources produced any tables. Check your SOURCES block."
        )

    result = {
        "contract_version": CONTRACT_VERSION,
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": source_results,
        "shared_columns": _shared_columns(source_results),
    }
    write_export(Path(output_path), result)
    log.info("stats.json written: %s", output_path)
    return result


# -- Top-level orchestration: discover mode -------------------------------


def _discover_handle(handle: SourceHandle) -> dict[str, Any]:
    """Metadata-only walk for one source handle.

    Pulls ``COUNT(*)`` (honoring any ``where`` clause on the source) and
    column metadata from the catalog, never the data. No samples, no
    distinct counts.
    """
    log.info("[%s] discover: counting rows...", handle.source_name)
    _flush_log_handlers()
    t0 = time.monotonic()
    n_rows = _count_rows(handle.conn, handle.table)
    log.info(
        "[%s] %d rows (%.1fs)",
        handle.source_name,
        n_rows,
        time.monotonic() - t0,
    )
    if handle.source_type == "sql":
        qualified = handle.source_detail.get("table")
        if not isinstance(qualified, str):
            raise RuntimeError(
                f"sql handle {handle.source_name!r} has no source_detail['table']"
            )
        columns = _describe_columns_sql(handle.conn, qualified)
    else:
        columns = _describe_columns_duckdb(handle.conn, handle.table)
    log.info("[%s] discover: %d columns", handle.source_name, len(columns))
    _flush_log_handlers()
    return {
        "source_name": handle.source_name,
        "source_type": handle.source_type,
        "source_detail": handle.source_detail,
        "row_count": n_rows,
        "columns": columns,
    }


def run_discover(
    sources: Iterable[Any],
    output_path: Path,
) -> dict[str, Any]:
    """Walk ``sources`` for metadata only and write ``discover.json``.

    SQL sources without ``tables=`` / ``pattern=`` / ``all=True`` are
    treated as "list everything reachable in this DSN" -- discover is
    the place where you don't yet know what to narrow to. Extract mode
    keeps the strict default.
    """
    sources = list(sources)
    log.info("run_discover: %d source declaration(s)", len(sources))
    _flush_log_handlers()
    source_results: list[dict[str, Any]] = []
    for src_idx, src in enumerate(sources, 1):
        log.info("source %d/%d: %r", src_idx, len(sources), src)
        _flush_log_handlers()
        for handle in iter_source(src, permissive=True):
            source_results.append(_discover_handle(handle))
            log.info(
                "source %d/%d: handle done (%d total handle(s) so far)",
                src_idx,
                len(sources),
                len(source_results),
            )
            _flush_log_handlers()

    if not source_results:
        raise RuntimeError(
            "No data sources produced any tables. Check your SOURCES block."
        )

    result = {
        "contract_version": DISCOVER_CONTRACT_VERSION,
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": source_results,
    }
    write_export(Path(output_path), result)
    log.info("discover.json written: %s", output_path)
    return result


# -- Public entry point ----------------------------------------------------


def main(
    sources: Iterable[Any],
    output_dir: Path,
    output_path: Path | None = None,
    *,
    mode: str = "discover",
    seed: int | None = None,
    classifier_seed: int = 0,
) -> dict[str, Any]:
    """Top-level orchestration. Dispatches on ``mode``.

    Args:
        sources: ``SOURCES`` returned from ``configure()``.
        output_dir: Directory for the output file (and for reading
            ``mdw_config.json`` in extract mode).
        output_path: Override the output filename. Defaults to
            ``discover.json`` (discover mode) or ``stats.json``
            (extract mode).
        mode: ``"discover"`` for the metadata-only walk that produces
            ``discover.json``, or ``"extract"`` for the typed pipeline
            that produces ``stats.json``. Extract mode requires a
            ``mdw_config.json`` next to ``output_dir``.
        seed: RNG seed for reproducible noise (extract only).
        classifier_seed: Seed for the per-column classification sample
            (extract only).

    Returns the result dict.
    """
    if mode not in ("discover", "extract"):
        raise ValueError(f"mode must be 'discover' or 'extract', got {mode!r}")
    sources = list(sources)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if mode == "discover":
        path = (
            Path(output_path)
            if output_path is not None
            else output_dir / "discover.json"
        )
        return run_discover(sources, path)

    config = load_config(output_dir)
    if config is None:
        raise RuntimeError(
            f"extract mode requires mdw_config.json next to the bundle "
            f"({output_dir}/mdw_config.json). Run mode='discover' first, "
            f"then mock-data-wizard configure on the resulting "
            f"discover.json."
        )
    log.info(
        "loaded mdw_config.json: %d type override(s), %d option override(s)",
        sum(len(v) for v in config.column_types.values()),
        sum(len(v) for v in config.column_options.values()),
    )

    path = Path(output_path) if output_path is not None else output_dir / "stats.json"
    return run_extract_typed(
        sources,
        path,
        config,
        seed=seed,
        classifier_seed=classifier_seed,
    )
