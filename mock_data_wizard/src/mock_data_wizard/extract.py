"""Entry points for the on-MONA discover and extract steps.

Two modes, one bundle:

- ``MODE = "discover"`` -- metadata-only walk over ``SOURCES``. SQL
  sources read ``INFORMATION_SCHEMA.COLUMNS`` and ``COUNT(*)``; file
  sources use DuckDB ``DESCRIBE`` and ``COUNT(*)``. No samples, no
  distinct counts. Output: ``mock_data_discovery.json``. The user copies
  it off MONA and authors ``project_data.json`` locally (the §15 step 7
  webapp owns the authoring UI; until then, hand-write against
  REFACTOR_SPEC.md §6).
- ``MODE = "extract"`` -- typed aggregation. Reads ``project_data.json``
  uploaded next to the bundle, requires every column to have a type
  override, and produces ``mock_data_stats.json``. No data-driven
  classifier pass: the configured type drives the per-column SQL.

PII discipline: only aggregate values cross the JSON boundary. Cell
suppression and noise live in :mod:`summarize`; this module just
orchestrates and routes the export through
:func:`reg_monabundle.scan.write_export`.
"""

from __future__ import annotations

import contextlib
import logging
import random
import re
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_monabundle.scan import write_export

from .sources import SourceHandle, iter_source
from .spec import LoadedSpec, load_project_data
from .sql_emit import DUCKDB, MSSQL, quote_ident
from .summarize import SUPPRESS_K, small_pop_threshold, summarize_column

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from reg_schema import Panel

log = logging.getLogger("mdw.extract")

CONTRACT_VERSION = "2.0.0"
DISCOVER_CONTRACT_VERSION = "discover-1.0.0"
DISCOVER_FILENAME = "mock_data_discovery.json"
STATS_FILENAME = "mock_data_stats.json"
SAMPLE_SIZE = 1000

# Year detection duplicates ``panels.detect_year_from_source_name``
# deliberately: this module is the on-MONA bundle root (amalgamated by
# ``reg_monabundle.build``) and ``mock_data_wizard.BUNDLE_MODULE_ORDER``
# does not include ``panels`` to keep the artifact small. Keep these
# byte-identical to ``panels``.
_YEAR_RE = re.compile(r"\d{4}")
_TERM_YEAR_RE = re.compile(r"(?:HT|VT)(\d{2})(?!\d)")


def _expand_term_year(two_digit: int) -> int:
    return 2000 + two_digit if two_digit < 70 else 1900 + two_digit


def _extract_year(name: str) -> int | None:
    y4 = _YEAR_RE.search(name)
    term = _TERM_YEAR_RE.search(name)
    if y4 is None and term is None:
        return None
    if term is None:
        assert y4 is not None
        return int(y4.group())
    if y4 is None or term.start() < y4.start():
        return _expand_term_year(int(term.group(1)))
    return int(y4.group())


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
    we don't have row-level access to. ``sources.py`` always emits
    schema-qualified names for SQL handles, so the unqualified case
    isn't reachable.
    """
    schema, _, name = qualified.partition(".")
    if not name:
        raise ValueError(
            f"sql table reference must be schema-qualified, got {qualified!r}"
        )
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

    ``table`` is f-string interpolated, not parameterised: it is built
    by ``sources.iter_file_source`` from ``configure()``-supplied paths
    (typed in by the analyst editing the bundle) and DuckDB ``DESCRIBE``
    accepts a FROM-clause expression rather than a parameterised name,
    so ``?`` substitution would not work here.
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

    Always called -- both counts land in ``mock_data_stats.json`` and are not
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


# -- Panel extraction (#23) -----------------------------------------------


def _extract_time_key_member_periods(
    handle: SourceHandle, panel: Panel, time_key_column: str
) -> list[dict[str, Any]]:
    """Run the per-period GROUP BY for one column-member on this handle.

    SQL form:

        SELECT time_key_column AS period, COUNT(*) AS n_rows,
               COUNT(DISTINCT entity_key) AS n_entity_ids
        FROM <table> GROUP BY time_key_column ORDER BY time_key_column

    Periods with ``n_entity_ids < SUPPRESS_K`` are dropped: the
    aggregate identifies a tiny sub-cohort and would leak under k-
    anonymity. ``period`` is preserved as int when the time-key column
    is integral (years are the dominant case); non-integer values
    (date/quarter strings) are kept as ``str`` so sub-annual panels
    don't crash the run.
    """
    # Composite entity_keys (tuple) aren't yet supported in this extraction
    # path; structural validation should prevent reaching here with one.
    assert isinstance(panel.entity_key, str), (
        f"panel {panel.panel_id!r}: composite entity_key not supported in "
        "time-key panel extraction"
    )
    entity_key = panel.entity_key
    qcol_time = quote_ident(time_key_column, handle.dialect)
    qcol_entity = quote_ident(entity_key, handle.dialect)
    sql = (
        f"SELECT {qcol_time} AS period, COUNT(*) AS n_rows, "
        f"COUNT(DISTINCT {qcol_entity}) AS n_entity_ids "
        f"FROM {handle.table} GROUP BY {qcol_time} ORDER BY {qcol_time}"
    )
    cur = handle.conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
    out: list[dict[str, Any]] = []
    for r in rows:
        period_raw, n_rows, n_entity_ids = r[0], int(r[1]), int(r[2])
        if period_raw is None:
            continue  # NULL time_key bucket: leave out for clarity
        if n_entity_ids < SUPPRESS_K:
            log.warning(
                "panel %s period %r suppressed (n_entity_ids=%d < %d)",
                panel.panel_id,
                period_raw,
                n_entity_ids,
                SUPPRESS_K,
            )
            continue
        out.append(
            {
                "period": _coerce_period(period_raw),
                "source": handle.source_name,
                "n_rows": n_rows,
                "n_entity_ids": n_entity_ids,
            }
        )
    return out


def _coerce_period(value: Any) -> int | str:
    """Normalise a SQL time_key value to a JSON-stable scalar.

    Integers (incl. integer-valued strings like ``"2018"``) become
    ``int``; everything else is stringified. Keeps year panels unchanged
    while letting date/quarter time_keys (``"2019-Q1"``, ``"2019-01"``)
    survive without crashing the run.
    """
    if isinstance(value, bool):
        return str(value)  # avoid bool being read as int
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return value
    return str(value)


def _build_panels_block(
    spec: LoadedSpec,
    source_results: Sequence[dict[str, Any]],
    time_key_periods: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Assemble the ``panels`` array for mock_data_stats.json.

    ``time_key_periods`` is keyed by ``(panel_id, member.source,
    member.time_key)`` and carries pre-computed per-period stats for
    column-members (the ``GROUP BY time_key`` query result). The
    time_key is part of the key because the schema permits two
    members of the same panel to share a source with different
    time_key columns (§6.4 only forbids cross-panel source reuse).
    File-members read ``n_rows`` from the per-source row count and
    ``n_entity_ids`` from the entity-key column's ``n_distinct``.
    Suppression is uniform (drop periods where
    ``n_entity_ids < SUPPRESS_K``).
    """
    out: list[dict[str, Any]] = []
    sources_by_name = {s["source_name"]: s for s in source_results}
    for panel in spec.panels:
        by_period: list[dict[str, Any]] = []
        members_out: list[dict[str, Any]] = []
        for member in panel.members:
            src_data = sources_by_name.get(member.source)
            if src_data is None:
                log.warning(
                    "panel %s: member source %r not in extract output -- skipping",
                    panel.panel_id,
                    member.source,
                )
                continue
            members_out.append({"source": member.source, "time_key": member.time_key})
            if isinstance(member.time_key, str):
                # Column-member: by-period stats produced by the
                # GROUP BY query at handle-processing time.
                by_period.extend(
                    time_key_periods.get(
                        (panel.panel_id, member.source, member.time_key), []
                    )
                )
                continue
            # File-member (int time_key): one period from the source-level
            # row count.
            col = next(
                (
                    c
                    for c in src_data["columns"]
                    if c["column_name"] == panel.entity_key
                ),
                None,
            )
            if col is None:
                raise RuntimeError(
                    f"panel {panel.panel_id!r}: source {member.source!r} has no "
                    f"column {panel.entity_key!r} (declared as entity_key)"
                )
            n_entity_ids = int(col["n_distinct"])
            if n_entity_ids < SUPPRESS_K:
                log.warning(
                    "panel %s period %d suppressed (n_entity_ids=%d < %d)",
                    panel.panel_id,
                    member.time_key,
                    n_entity_ids,
                    SUPPRESS_K,
                )
                continue
            by_period.append(
                {
                    "period": member.time_key,
                    "source": member.source,
                    "n_rows": int(src_data["row_count"]),
                    "n_entity_ids": n_entity_ids,
                }
            )
        if not members_out:
            # Every declared member was missing from the extract -- that
            # is a configuration error (typos, filtered-out sources),
            # not a routine suppression case. Surface it here rather
            # than silently producing a stats payload that would fail
            # downstream schema validation with a less actionable error.
            raise RuntimeError(
                f"panel {panel.panel_id!r}: no member sources matched the "
                f"extract output (declared: "
                f"{[m.source for m in panel.members]!r})"
            )
        out.append(
            {
                "panel_id": panel.panel_id,
                "entity_key": panel.entity_key,
                "members": members_out,
                "by_period": by_period,
            }
        )
    return out


# -- Per-handle pipeline (extract mode) -----------------------------------


def process_handle(
    handle: SourceHandle,
    rng: random.Random,
    spec: LoadedSpec,
    *,
    classifier_seed: int = 0,
) -> dict[str, Any]:
    """Process one :class:`SourceHandle` into a source-level stats dict.

    Every column must carry a type override in ``spec``; this is the
    only entry point and it has no classifier fallback. Subtype /
    date-format detection still runs the per-column sample when the
    override has no inline hint -- ``classifier_seed`` controls that.
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

    # Validate the full column set up front so the user sees every
    # missing override at once instead of one error per re-run.
    missing = [c for c in cols if spec.lookup_type(handle.source_name, c) is None]
    if missing:
        listed = ", ".join(repr(c) for c in missing)
        raise RuntimeError(
            f"extract mode: source {handle.source_name!r} has "
            f"{len(missing)} column(s) with no type override in "
            f"project_data.json: {listed}. Add the missing entries by "
            f"hand (or regenerate from a fresh discover payload via the "
            f"§15 step 7 webapp once it lands)."
        )

    columns_out: list[dict[str, Any]] = []
    for i, col in enumerate(cols, 1):
        t_col = time.monotonic()
        n_distinct, null_count = _count_distinct_and_nulls(
            handle.conn, handle.table, col, handle.dialect
        )

        override = spec.lookup_type(handle.source_name, col)
        assert override is not None  # validated above
        options = spec.lookup_options(handle.source_name, col)
        # Skip the per-column sample query when an inline hint pins the
        # subtype/format -- nothing downstream consumes the sample then.
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

        columns_out.append(
            summarize_column(
                handle.conn,
                handle.table,
                col,
                override.type,
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
            "[%s] col %d/%d %s -> %s (%.1fs)",
            handle.source_name,
            i,
            len(cols),
            col,
            override.type,
            time.monotonic() - t_col,
        )
        _flush_log_handlers()

    detail = dict(handle.source_detail)
    year = _extract_year(handle.source_name)
    if year is not None:
        detail["year"] = year
    return {
        "source_name": handle.source_name,
        "source_type": handle.source_type,
        "source_detail": detail,
        "row_count": n_rows,
        "columns": columns_out,
    }


def _flush_log_handlers() -> None:
    for h in logging.getLogger().handlers:
        with contextlib.suppress(Exception):
            h.flush()


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
    spec: LoadedSpec,
    *,
    seed: int | None = None,
    classifier_seed: int = 0,
) -> dict[str, Any]:
    """Run the typed extract pipeline and write ``mock_data_stats.json``.

    Every column must carry a type override in ``spec`` -- this mode
    has no classifier fallback. Run ``discover`` + configure first to
    author the file. ``seed`` controls Python-side noise injection;
    ``classifier_seed`` controls the per-column sample for subtype /
    date-format detection (still used when the override is present
    without an inline hint).
    """
    rng = random.Random(seed)
    sources = list(sources)
    log.info("run_extract_typed: %d source declaration(s)", len(sources))
    _flush_log_handlers()
    # Index column-members up front so each handle can run its
    # GROUP BY time_key query while the connection is still open.
    # The schema permits two members of the *same* panel to share a
    # source with different time_key columns (reg_schema only forbids
    # cross-panel source reuse, §6.4) — index per-member so each
    # GROUP BY runs once and lookup is unambiguous in
    # ``_build_panels_block``.
    time_key_members_by_source: dict[str, list[tuple[Panel, str]]] = defaultdict(list)
    for panel in spec.panels:
        for member in panel.members:
            if isinstance(member.time_key, str):
                time_key_members_by_source[member.source].append(
                    (panel, member.time_key)
                )
    time_key_periods: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    source_results: list[dict[str, Any]] = []
    for src_idx, src in enumerate(sources, 1):
        log.info("source %d/%d: %r", src_idx, len(sources), src)
        _flush_log_handlers()
        for handle in iter_source(src, spec=spec):
            source_results.append(
                process_handle(
                    handle,
                    rng,
                    spec,
                    classifier_seed=classifier_seed,
                )
            )
            for panel, time_key in time_key_members_by_source.get(
                handle.source_name, []
            ):
                time_key_periods[(panel.panel_id, handle.source_name, time_key)] = (
                    _extract_time_key_member_periods(handle, panel, time_key)
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
        "generated_at": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": source_results,
        "shared_columns": _shared_columns(source_results),
        "panels": _build_panels_block(spec, source_results, time_key_periods),
    }
    write_export(Path(output_path), result)
    log.info("mock_data_stats.json written: %s", output_path)
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
    detail = dict(handle.source_detail)
    year = _extract_year(handle.source_name)
    if year is not None:
        detail["year"] = year
    return {
        "source_name": handle.source_name,
        "source_type": handle.source_type,
        "source_detail": detail,
        "row_count": n_rows,
        "columns": columns,
    }


def run_discover(
    sources: Iterable[Any],
    output_path: Path,
) -> dict[str, Any]:
    """Walk ``sources`` for metadata only and write ``mock_data_discovery.json``.

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
        "generated_at": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": source_results,
    }
    write_export(Path(output_path), result)
    log.info("mock_data_discovery.json written: %s", output_path)
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
    spec: LoadedSpec | None = None,
) -> dict[str, Any]:
    """Top-level orchestration. Dispatches on ``mode``.

    Args:
        sources: ``SOURCES`` returned from ``configure()``.
        output_dir: Directory for the output file (and for reading
            ``project_data.json`` in extract mode when ``spec`` is None).
        output_path: Override the output filename. Defaults to
            ``mock_data_discovery.json`` (discover mode) or
            ``mock_data_stats.json`` (extract mode).
        mode: ``"discover"`` for the metadata-only walk that produces
            ``mock_data_discovery.json``, or ``"extract"`` for the typed
            pipeline that produces ``mock_data_stats.json``.
        seed: RNG seed for reproducible noise (extract only).
        classifier_seed: Seed for the per-column classification sample
            (extract only).
        spec: Pre-loaded ``LoadedSpec`` (e.g. parsed from a bundle's
            embedded ``project_data.json``). When ``None`` in extract
            mode, the spec is loaded from ``output_dir/project_data.json``.

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
            else output_dir / DISCOVER_FILENAME
        )
        return run_discover(sources, path)

    if spec is None:
        spec = load_project_data(output_dir)
    if spec is None:
        raise RuntimeError(
            f"extract mode requires project_data.json next to the bundle "
            f"({output_dir}/project_data.json). Run mode='discover' first, "
            f"then author project_data.json locally against REFACTOR_SPEC.md "
            f"§6 (the §15 step 7 webapp owns this authoring once it lands)."
        )
    log.info(
        "loaded project_data.json: %d source(s), %d panel(s)",
        len(spec.project_data.sources),
        len(spec.project_data.panels),
    )

    path = Path(output_path) if output_path is not None else output_dir / STATS_FILENAME
    return run_extract_typed(
        sources,
        path,
        spec,
        seed=seed,
        classifier_seed=classifier_seed,
    )
