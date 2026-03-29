"""Enrich stats with regmeta registry metadata."""

from __future__ import annotations

import math
import signal
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import regmeta

from ._util import progress, strip_project_prefix
from .stats import ColumnStats, ProjectStats

# Birth-invariant regmeta var_ids eligible for population spine.
# These attributes are fixed at birth and must be consistent per individual.
SPINE_VAR_IDS = frozenset({44, 1378, 256, 257})
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
    register_id: int | None = None
    var_id: int | None = None
    variable_name: str | None = None
    value_codes: dict[str, str] | None = None  # code -> label


@dataclass
class EnrichedFile:
    file_name: str
    relative_path: str
    row_count: int
    columns: list[EnrichedColumn]
    register_hint: int | None = None


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
    prev_handler = None
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

        # Per-file resolved vars and the register each file votes for
        file_resolved: dict[str, dict[str, _ResolvedVar]] = {}
        file_register: dict[str, int | None] = {}
        value_codes: dict[int, dict[str, str]] = {}

        if conn is not None:
            if register:
                # Explicit register: single pass, all files use it
                reg_ids = regmeta.resolve_register_ids(conn, register)
                global_resolved = _bulk_resolve(conn, all_col_names, reg_ids or None)
                for file_stats in stats.files:
                    file_resolved[file_stats.file_name] = global_resolved
                    file_register[file_stats.file_name] = reg_ids[0] if reg_ids else None
            else:
                # Two-pass: vote on register per file, then resolve within it
                col_to_registers = _bulk_resolve_all_registers(conn, all_col_names)

                # Group files by their voted register so we batch DB queries
                register_to_files: dict[int | None, list[str]] = {}
                for file_stats in stats.files:
                    col_names = [c.column_name for c in file_stats.columns]
                    voted = _vote_register(col_names, col_to_registers, file_stats.file_name)
                    file_register[file_stats.file_name] = voted
                    register_to_files.setdefault(voted, []).append(file_stats.file_name)

                # One _bulk_resolve per distinct voted register
                for reg_id, _fnames in register_to_files.items():
                    reg_ids = [reg_id] if reg_id is not None else None
                    resolved = _bulk_resolve(conn, all_col_names, reg_ids)
                    for fname in _fnames:
                        file_resolved[fname] = resolved

            # Collect categorical var_ids for value code fetch
            cat_var_ids: set[int] = set()
            for file_stats in stats.files:
                resolved = file_resolved.get(file_stats.file_name, {})
                for col in file_stats.columns:
                    rv = resolved.get(col.column_name.lower()) or resolved.get(
                        strip_project_prefix(col.column_name).lower()
                    )
                    if col.inferred_type == "categorical" and rv is not None:
                        cat_var_ids.add(rv.var_id)
            if cat_var_ids:
                value_codes = _bulk_fetch_value_codes(conn, cat_var_ids)

        matched_total = 0
        enriched_files: list[EnrichedFile] = []
        for file_stats in stats.files:
            resolved = file_resolved.get(file_stats.file_name, {})
            enriched_cols = []
            for col in file_stats.columns:
                ecol = _column_from_stats(col)
                rv = resolved.get(ecol.column_name.lower()) or resolved.get(
                    strip_project_prefix(ecol.column_name).lower()
                )
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
                    register_hint=file_register.get(file_stats.file_name),
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
        for w in _check_value_code_drift(enriched_files):
            progress(f"  Warning: {w}")

    return enriched_files


def _check_value_code_drift(enriched_files: list[EnrichedFile]) -> list[str]:
    """Warn when stats contain frequency codes absent from regmeta value codes."""
    warnings: list[str] = []
    for ef in enriched_files:
        for ec in ef.columns:
            if ec.inferred_type != "categorical" or not ec.value_codes:
                continue
            freq_keys = set(ec.stats.get("frequencies", {})) - {"_other"}
            unknown = sorted(freq_keys - set(ec.value_codes))
            if unknown:
                codes = ", ".join(unknown)
                warnings.append(
                    f"{ef.file_name}/{ec.column_name}: "
                    f"codes [{codes}] not in regmeta value set"
                )
    return warnings


# ---------------------------------------------------------------------------
# Bulk DB queries — bypass general-purpose regmeta API for performance
# ---------------------------------------------------------------------------


@dataclass
class _ResolvedVar:
    register_id: int
    var_id: int
    variable_name: str


def _bulk_resolve_all_registers(
    conn: sqlite3.Connection,
    col_names: set[str],
) -> dict[str, list[int]]:
    """Resolve each column to ALL matching register_ids.

    Returns {lowercase_col_name: [register_id, ...]}. Used for majority-vote
    register detection before the targeted per-register resolve.
    """
    lookup_names = set(col_names)
    for c in col_names:
        stripped = strip_project_prefix(c)
        if stripped != c:
            lookup_names.add(stripped)

    col_list = sorted(lookup_names)
    placeholders = ",".join("?" for _ in col_list)
    sql = (
        "SELECT LOWER(va.kolumnnamn) AS col, vi.register_id "
        "FROM variable_alias va "
        "JOIN variable_instance vi ON va.cvid = vi.cvid "
        f"WHERE LOWER(va.kolumnnamn) IN ({placeholders}) "
        "GROUP BY LOWER(va.kolumnnamn), vi.register_id"
    )
    rows = conn.execute(sql, [c.lower() for c in col_list]).fetchall()

    result: dict[str, list[int]] = {}
    for r in rows:
        result.setdefault(r["col"], []).append(r["register_id"])
    return result


# Standard SCB delivery tables whose filenames reliably indicate the register.
# Used as fallback when the column-based vote is inconclusive.
_SCB_TABLE_REGISTER: dict[str, int] = {
    "fodelseuppg": 2,       # RTB
    "immigranter": 2,       # RTB
    "population": 2,        # RTB (Population_PersonNr_*)
    "flergen": 349,         # Flergenerationsregistret
}


def _filename_register_fallback(file_name: str) -> int | None:
    """Match known SCB delivery table names to register IDs."""
    stem = file_name.rsplit(".", 1)[0].lower()
    for prefix, reg_id in _SCB_TABLE_REGISTER.items():
        if stem.startswith(prefix):
            return reg_id
    return None


def _vote_register(
    file_col_names: list[str],
    col_to_registers: dict[str, list[int]],
    file_name: str = "",
) -> int | None:
    """Pick the best-fit register for a file via weighted majority vote.

    Generic columns (appearing in many registers) are downweighted to avoid
    noise from Kommun/Kön/Ar which exist in 70-120 registers.
    Falls back to known SCB delivery table names if the vote is inconclusive.
    """
    votes: Counter[int] = Counter()
    for raw_col in file_col_names:
        col = strip_project_prefix(raw_col).lower()
        regs = col_to_registers.get(col)
        if not regs:
            continue
        weight = 1.0 / math.log2(max(len(regs), 2))
        for reg_id in regs:
            votes[reg_id] += weight

    if not votes:
        return _filename_register_fallback(file_name)

    top = votes.most_common(2)
    winner_id, winner_score = top[0]
    if len(top) > 1:
        _, runner_up_score = top[1]
        # Require either a clear lead or at least 3 weighted votes
        if winner_score < runner_up_score * 1.2 and winner_score < 3:
            return _filename_register_fallback(file_name)
    return winner_id


def _bulk_resolve(
    conn: sqlite3.Connection,
    col_names: set[str],
    register_ids: list[int] | None = None,
) -> dict[str, _ResolvedVar]:
    """Resolve column names. When register_ids is given, constrain to those registers."""
    reg_filter = ""
    params: list[Any] = []
    if register_ids:
        placeholders = ",".join("?" for _ in register_ids)
        reg_filter = f" AND vi.register_id IN ({placeholders})"
        params.extend(register_ids)

    # Include stripped versions so P1105_LopNr → LopNr matches
    lookup_names = set(col_names)
    for c in col_names:
        stripped = strip_project_prefix(c)
        if stripped != c:
            lookup_names.add(stripped)

    col_list = sorted(lookup_names)
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
    var_ids: set[int],
) -> dict[int, dict[str, str]]:
    """Fetch value codes for a set of var_ids. Returns var_id -> {code: label}.

    For each var_id, picks the CVID with the most value codes.
    """
    if not var_ids:
        return {}

    # Find best CVID per var_id (the one with the most value codes)
    var_list = sorted(var_ids)
    placeholders = ",".join("?" for _ in var_list)
    best_cvids = conn.execute(
        "SELECT vi.var_id, vi.cvid, COUNT(*) as cnt "
        "FROM variable_instance vi "
        "JOIN cvid_value_code cvc ON vi.cvid = cvc.cvid "
        f"WHERE vi.var_id IN ({placeholders}) "
        "GROUP BY vi.var_id, vi.cvid "
        "ORDER BY vi.var_id, cnt DESC",
        var_list,
    ).fetchall()

    # Pick best CVID per var_id
    var_to_cvid: dict[int, int] = {}
    for r in best_cvids:
        if r["var_id"] not in var_to_cvid:
            var_to_cvid[r["var_id"]] = r["cvid"]

    if not var_to_cvid:
        return {}

    # Fetch all value codes for the selected CVIDs in one query
    cvid_list = sorted(set(var_to_cvid.values()))
    placeholders = ",".join("?" for _ in cvid_list)
    value_rows = conn.execute(
        "SELECT cvc.cvid, vc.vardekod, vc.vardebenamning "
        "FROM cvid_value_code cvc "
        "JOIN value_code vc ON cvc.code_id = vc.code_id "
        f"WHERE cvc.cvid IN ({placeholders}) "
        "ORDER BY cvc.cvid, vc.vardekod",
        cvid_list,
    ).fetchall()

    # Group by CVID, filtering out SCB type-hint codes
    cvid_to_codes: dict[int, dict[str, str]] = {}
    for r in value_rows:
        if r["vardekod"] not in _SCB_TYPE_HINTS:
            cvid_to_codes.setdefault(r["cvid"], {})[r["vardekod"]] = r["vardebenamning"]

    # Map back to var_id (skip empty or single-code sets — a lone code
    # is never a useful categorical universe)
    result: dict[int, dict[str, str]] = {}
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
