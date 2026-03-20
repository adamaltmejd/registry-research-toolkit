"""Enrich stats with regmeta registry metadata."""

from __future__ import annotations

import signal
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import regmeta

from ._util import progress
from .stats import ColumnStats, ProjectStats

# Birth-invariant regmeta var_ids eligible for population spine.
# These attributes are fixed at birth and must be consistent per individual.
SPINE_VAR_IDS = frozenset({"44", "1378", "256", "257"})
# 44 = Kön, 1378 = Födelseår, 256 = Födelselän, 257 = Födelseland


@dataclass
class EnrichedColumn:
    column_name: str
    inferred_type: str
    nullable: bool
    null_rate: float
    n_distinct: int
    stats: dict[str, Any]
    # Enrichment from regmeta
    register_id: str | None = None
    var_id: str | None = None
    variable_name: str | None = None
    value_codes: dict[str, str] | None = None  # code -> label


@dataclass
class EnrichedFile:
    file_name: str
    relative_path: str
    row_count: int
    columns: list[EnrichedColumn]


def _column_from_stats(col: ColumnStats) -> EnrichedColumn:
    return EnrichedColumn(
        column_name=col.column_name,
        inferred_type=col.inferred_type,
        nullable=col.nullable,
        null_rate=col.null_rate,
        n_distinct=col.n_distinct,
        stats=col.stats,
    )


def enrich(
    stats: ProjectStats,
    *,
    register: str | None = None,
    db_path: Path | None = None,
) -> list[EnrichedFile]:
    """Combine stats with regmeta metadata.

    If db_path is provided, opens the regmeta database and uses it to resolve
    column names and fetch value codes. Raises if the db cannot be opened.
    If db_path is None, returns unenriched results.
    """
    conn: sqlite3.Connection | None = None
    _cancelled = False
    prev_handler = signal.getsignal(signal.SIGINT)
    if db_path is not None:
        conn = regmeta.open_db(db_path)

        # Allow Ctrl+C to interrupt long-running SQLite queries.
        # Python signal handlers can't run while blocked in C extensions,
        # so we use SQLite's progress handler which is called periodically
        # during query execution. The SIGINT handler sets a flag, and the
        # progress handler checks it and aborts the query.
        prev_handler = signal.getsignal(signal.SIGINT)

        def _sigint_handler(sig: int, frame: object) -> None:
            nonlocal _cancelled
            _cancelled = True

        def _progress_handler() -> int:
            return 1 if _cancelled else 0

        signal.signal(signal.SIGINT, _sigint_handler)
        conn.set_progress_handler(_progress_handler, 10000)

    total = len(stats.files)
    if conn is not None:
        progress(f"Enriching {total} files with regmeta...")

    t0 = time.monotonic()

    try:
        # Collect all unique column names across all files
        all_col_names: set[str] = set()
        for file_stats in stats.files:
            for col in file_stats.columns:
                all_col_names.add(col.column_name)

        # Bulk resolve + bulk value code fetch
        resolved: dict[str, _ResolvedVar] = {}
        value_codes: dict[str, dict[str, str]] = {}
        if conn is not None:
            resolved = _bulk_resolve(conn, all_col_names, register)
            cat_var_ids: set[str] = set()
            for file_stats in stats.files:
                for col in file_stats.columns:
                    rv = resolved.get(col.column_name.lower())
                    if col.inferred_type == "categorical" and rv is not None:
                        cat_var_ids.add(rv.var_id)
            if cat_var_ids:
                value_codes = _bulk_fetch_value_codes(conn, cat_var_ids)

        matched_total = 0
        enriched_files: list[EnrichedFile] = []
        for file_stats in stats.files:
            enriched_cols = []
            for col in file_stats.columns:
                ecol = _column_from_stats(col)
                rv = resolved.get(ecol.column_name.lower())
                if rv is not None:
                    ecol.register_id = rv.register_id
                    ecol.var_id = rv.var_id
                    ecol.variable_name = rv.variable_name
                    matched_total += 1
                    if ecol.inferred_type == "categorical" and rv.var_id in value_codes:
                        ecol.value_codes = value_codes[rv.var_id]
                enriched_cols.append(ecol)

            enriched_files.append(
                EnrichedFile(
                    file_name=file_stats.file_name,
                    relative_path=file_stats.relative_path,
                    row_count=file_stats.row_count,
                    columns=enriched_cols,
                )
            )
    except sqlite3.OperationalError as exc:
        if _cancelled or "interrupt" in str(exc).lower():
            raise KeyboardInterrupt from None
        raise
    finally:
        if conn is not None:
            conn.set_progress_handler(None, 0)
            signal.signal(signal.SIGINT, prev_handler)
            conn.close()

    if conn is not None:
        elapsed = time.monotonic() - t0
        total_cols = sum(len(f.columns) for f in enriched_files)
        progress(
            f"Enriched {total} files ({matched_total}/{total_cols} columns matched) "
            f"in {elapsed:.1f}s"
        )

    return enriched_files


# ---------------------------------------------------------------------------
# Bulk DB queries — bypass general-purpose regmeta API for performance
# ---------------------------------------------------------------------------


@dataclass
class _ResolvedVar:
    register_id: str
    var_id: str
    variable_name: str


def _bulk_resolve(
    conn: sqlite3.Connection,
    col_names: set[str],
    register: str | None,
) -> dict[str, _ResolvedVar]:
    """Resolve all column names in one query. Returns first match per column."""
    reg_filter = ""
    params: list[str] = []
    if register:
        reg_ids = regmeta.resolve_register_ids(conn, register)
        if not reg_ids:
            return {}
        placeholders = ",".join("?" for _ in reg_ids)
        reg_filter = f" AND vi.register_id IN ({placeholders})"
        params.extend(reg_ids)

    # Single query: resolve all column names at once
    col_list = sorted(col_names)
    placeholders = ",".join("?" for _ in col_list)
    sql = (
        "SELECT va.kolumnnamn, vi.register_id, vi.var_id, v.variabelnamn "
        "FROM variable_alias va "
        "JOIN variable_instance vi ON va.cvid = vi.cvid "
        "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
        f"WHERE LOWER(va.kolumnnamn) IN ({placeholders})"
        f"{reg_filter} "
        "GROUP BY LOWER(va.kolumnnamn), vi.register_id, vi.var_id "
        "ORDER BY va.kolumnnamn, vi.register_id"
    )
    rows = conn.execute(sql, [c.lower() for c in col_list] + params).fetchall()

    # Keep first match per column name, keyed by lowercase for lookup
    result: dict[str, _ResolvedVar] = {}
    for r in rows:
        key = r["kolumnnamn"].lower()
        if key not in result:
            result[key] = _ResolvedVar(
                register_id=r["register_id"],
                var_id=r["var_id"],
                variable_name=r["variabelnamn"],
            )
    return result


def _bulk_fetch_value_codes(
    conn: sqlite3.Connection,
    var_ids: set[str],
) -> dict[str, dict[str, str]]:
    """Fetch value codes for a set of var_ids. Returns var_id -> {code: label}.

    For each var_id, picks the CVID with the most value items.
    """
    if not var_ids:
        return {}

    # Find best CVID per var_id (the one with the most value items)
    var_list = sorted(var_ids)
    placeholders = ",".join("?" for _ in var_list)
    best_cvids = conn.execute(
        "SELECT vi.var_id, vi.cvid, COUNT(val.value_item_id) as cnt "
        "FROM variable_instance vi "
        "JOIN value_item val ON vi.cvid = val.cvid "
        f"WHERE vi.var_id IN ({placeholders}) "
        "GROUP BY vi.var_id, vi.cvid "
        "ORDER BY vi.var_id, cnt DESC",
        var_list,
    ).fetchall()

    # Pick best CVID per var_id
    var_to_cvid: dict[str, str] = {}
    for r in best_cvids:
        if r["var_id"] not in var_to_cvid:
            var_to_cvid[r["var_id"]] = r["cvid"]

    if not var_to_cvid:
        return {}

    # Fetch all value items for the selected CVIDs in one query
    cvid_list = sorted(set(var_to_cvid.values()))
    placeholders = ",".join("?" for _ in cvid_list)
    value_rows = conn.execute(
        "SELECT cvid, vardekod, vardebenamning "
        f"FROM value_item WHERE cvid IN ({placeholders}) "
        "ORDER BY cvid, vardekod",
        cvid_list,
    ).fetchall()

    # Group by CVID, filtering out SCB type-hint codes
    cvid_to_codes: dict[str, dict[str, str]] = {}
    for r in value_rows:
        if r["vardekod"] not in _SCB_TYPE_HINTS:
            cvid_to_codes.setdefault(r["cvid"], {})[r["vardekod"]] = r["vardebenamning"]

    # Map back to var_id (skip empty or single-code sets — a lone code
    # is never a useful categorical universe)
    result: dict[str, dict[str, str]] = {}
    for var_id, cvid in var_to_cvid.items():
        codes = cvid_to_codes.get(cvid)
        if codes and len(codes) > 1:
            result[var_id] = codes

    return result


# SCB metadata type hints — these describe the column's data type,
# not valid categorical values. Filtering them prevents generating
# nonsense like all-"Tal" for numeric columns.
_SCB_TYPE_HINTS = frozenset(
    {
        "Tal",
        "Beskrivande text",
        "Continuous value code",
    }
)
