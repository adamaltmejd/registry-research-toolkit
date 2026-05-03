"""Entry point for the on-MONA extract step.

Drives the pipeline:

    SOURCES = [...]                            (declared by the user)
        -> per source: iterate :class:`SourceHandle`s (one per table)
        -> per handle: COUNT(*), list columns, pre-classify, summarise
        -> aggregate into ``stats.json``

Discovery: if any source has no filtering info, the script writes a
sidecar ``mdw_sources_<TS>.py`` listing everything discoverable and
exits before running any aggregation. The user narrows the file in
place; the next run picks up the latest sidecar automatically (it
overrides the in-script ``SOURCES``).

PII discipline: only aggregate values cross the JSON boundary. Cell
suppression and noise live in :mod:`summarize`; this module just
orchestrates.
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
from .sources import (
    FileSource,
    SourceHandle,
    SqlSource,
    _is_archived,
    file_source,
    iter_source,
    list_files_in_source,
    list_sql_views,
    needs_discovery,
    sql_connect,
    sql_source,
    sql_table,
)
from .sql_emit import DUCKDB, MSSQL, quote_ident
from .summarize import small_pop_threshold, summarize_column

log = logging.getLogger("mdw.extract")

CONTRACT_VERSION = "2.0.0"
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


# -- Per-handle pipeline ---------------------------------------------------


def process_handle(
    handle: SourceHandle,
    rng: random.Random,
    *,
    classifier_seed: int = 0,
    config: MDWConfig | None = None,
) -> dict[str, Any]:
    """Process one :class:`SourceHandle` into a source-level stats dict."""
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
    log.info("[%s] %d columns to classify", handle.source_name, len(cols))
    _flush_log_handlers()

    columns_out: list[dict[str, Any]] = []
    for i, col in enumerate(cols, 1):
        t_col = time.monotonic()
        n_distinct, null_count = _count_distinct_and_nulls(
            handle.conn, handle.table, col, handle.dialect
        )

        override = config.lookup_type(handle.source_name, col) if config else None
        options = config.lookup_options(handle.source_name, col) if config else {}
        if override is not None:
            col_type = override.type
            source_of_type = "override"
            # Inline subtype/format hint -> sample is unused downstream;
            # skip the per-column scan entirely.
            sample: list[Any] = (
                []
                if override.has_inline_hint()
                else _sample_values(
                    handle.conn,
                    handle.table,
                    col,
                    handle.dialect,
                    seed=classifier_seed,
                )
            )
        else:
            sample = _sample_values(
                handle.conn,
                handle.table,
                col,
                handle.dialect,
                seed=classifier_seed,
            )
            col_type = classify_column(col, n_rows, n_distinct, sample)
            source_of_type = "auto"

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
                source_of_type=source_of_type,
                id_subtype=override.id_subtype if override else None,
                numeric_subtype=override.numeric_subtype if override else None,
                date_format=override.date_format if override else None,
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
            source_of_type,
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


# -- Top-level orchestration ----------------------------------------------


def run_extract(
    sources: Iterable[Any],
    output_path: Path,
    *,
    seed: int | None = None,
    classifier_seed: int = 0,
    config: MDWConfig | None = None,
) -> dict[str, Any]:
    """Run the full extract pipeline and write ``output_path``.

    Returns the parsed ``stats.json`` dict (callers can also just read
    the file).

    ``seed`` controls Python-side noise injection in :mod:`summarize`.
    ``classifier_seed`` controls the per-column classification sample
    (DuckDB ``REPEATABLE`` reservoir); same data + same seed always
    classifies the same way. ``config`` carries optional per-column
    type overrides loaded from ``mdw_config.json``.
    """
    rng = random.Random(seed)
    sources = list(sources)
    log.info("run_extract: %d source declaration(s)", len(sources))
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


# -- Discovery mode --------------------------------------------------------


def _format_file_where_arg(where: Any) -> str:
    if where is None:
        return ""
    return f"        where={where!r},\n"


def _format_file_skeleton(src: FileSource) -> str:
    files = list_files_in_source(src)
    basenames = sorted({f.name for f in files})
    if not basenames:
        return f"    # file_source(path={src.path!r}) -- no matching files"
    inc_lines = ",\n".join(f"            {b!r}" for b in basenames)
    return (
        f"    file_source(\n"
        f"        path={src.path!r},\n"
        f"        include=(\n{inc_lines},\n        ),\n"
        f"{_format_file_where_arg(src.where)}"
        f"    ),"
    )


def _format_sql_skeleton(src: SqlSource) -> str:
    try:
        conn = sql_connect(src)
        try:
            tables = list_sql_views(conn, src)
        finally:
            conn.close()
    except Exception as e:
        return (
            f"    # sql_source(dsn={src.dsn!r}) discovery failed: "
            f"{type(e).__name__}: {e}"
        )
    if src.exclude_archived:
        tables = [t for t in tables if not _is_archived(t)]
    if not tables:
        return f"    # sql_source(dsn={src.dsn!r}) -- no matching tables"
    # Plain strings keep the skeleton compact. To attach a WHERE filter,
    # the user wraps a row in sql_table(..., where=...) after narrowing.
    tbl_lines = ",\n".join(f"            {t!r}" for t in tables)
    return (
        f"    sql_source(\n"
        f"        dsn={src.dsn!r},\n"
        f"        tables=(\n{tbl_lines},\n        ),\n"
        f"        # Attach a per-table cohort filter by wrapping a row, e.g.:\n"
        f"        #   sql_table('dbo.persons', where='AR > 2015'),\n"
        f"    ),"
    )


def emit_sources_skeleton(sources: Iterable[Any], output_dir: Path) -> Path:
    """Write a ``mdw_sources_<TS>.py`` skeleton next to the script."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"mdw_sources_{timestamp}.py"

    parts = [
        "# Auto-generated by mock-data-wizard discovery.",
        "# Edit this file to narrow what gets aggregated, then re-run.",
        "# Delete this file (and any sibling mdw_sources_*.py) to re-discover.",
        "",
        "SOURCES = [",
    ]
    for src in sources:
        if isinstance(src, FileSource):
            parts.append(_format_file_skeleton(src))
        elif isinstance(src, SqlSource):
            parts.append(_format_sql_skeleton(src))
        else:
            parts.append(f"    # unknown source skipped: {src!r}")
    parts.append("]")
    output_path.write_text("\n".join(parts) + "\n", encoding="utf-8")
    return output_path


def find_latest_sources_file(directory: Path) -> Path | None:
    candidates = sorted(Path(directory).glob("mdw_sources_*.py"))
    return candidates[-1] if candidates else None


def load_sources_file(path: Path) -> list[Any]:
    """Exec a sidecar ``mdw_sources_*.py`` and return its ``SOURCES``."""
    code = Path(path).read_text(encoding="utf-8")
    ns: dict[str, Any] = {
        "file_source": file_source,
        "sql_source": sql_source,
        "sql_table": sql_table,
    }
    exec(compile(code, str(path), "exec"), ns)
    return list(ns.get("SOURCES", []))


# -- Public entry point ----------------------------------------------------


def main(
    sources: Iterable[Any],
    output_dir: Path,
    output_path: Path | None = None,
    *,
    seed: int | None = None,
    classifier_seed: int = 0,
) -> dict[str, Any] | None:
    """Top-level orchestration.

    Args:
        sources: In-script SOURCES (used unless a sidecar overrides).
        output_dir: Directory for ``stats.json`` and sidecar files.
        output_path: Override path for ``stats.json``.
        seed: RNG seed for reproducible noise (omit for non-determinism).
        classifier_seed: Seed for the per-column classification sample.
            Same data + same seed always produces the same classifications;
            varying the seed is permitted to differ.

    Loads ``mdw_config.json`` from ``output_dir`` if present (per-column
    type and option overrides). Strict on schema violations -- typos in
    the config block extract rather than getting silently ignored.

    Returns the stats dict on a successful run, or ``None`` if discovery
    mode wrote a sidecar and exited.
    """
    sources = list(sources)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = (
        Path(output_path) if output_path is not None else output_dir / "stats.json"
    )

    sidecar = find_latest_sources_file(output_dir)
    if sidecar is not None:
        log.info("loading SOURCES override from %s", sidecar)
        sources = load_sources_file(sidecar)
        if any(needs_discovery(s) for s in sources):
            raise RuntimeError(
                f"Sidecar {sidecar} still has discovery-mode source(s). "
                "Narrow with include/tables/pattern, set all=True, or "
                "delete the file to re-discover."
            )
    elif any(needs_discovery(s) for s in sources):
        log.info("discovery mode: scanning %d source(s)", len(sources))
        path = emit_sources_skeleton(sources, output_dir)
        log.info(
            "wrote %s -- edit to narrow scope, then re-run. No stats.json written.",
            path,
        )
        return None

    config = load_config(output_dir)
    if config is not None:
        log.info(
            "loaded mdw_config.json: %d type override(s), %d option override(s)",
            sum(len(v) for v in config.column_types.values()),
            sum(len(v) for v in config.column_options.values()),
        )

    return run_extract(
        sources,
        output_path,
        seed=seed,
        classifier_seed=classifier_seed,
        config=config,
    )
