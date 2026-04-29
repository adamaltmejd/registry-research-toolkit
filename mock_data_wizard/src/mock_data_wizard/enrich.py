"""Enrich stats with regmeta registry metadata."""

from __future__ import annotations

import math
import signal
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass, field
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
class RegisterCandidate:
    """A plausible source register for a file, with match evidence."""

    register_id: int
    match_count: int
    total_nonid_cols: int


@dataclass
class EnrichedSource:
    source_name: str
    source_type: str
    source_detail: dict[str, Any]
    row_count: int
    columns: list[EnrichedColumn]
    register_hint: int | None = None
    register_hint_candidates: list[RegisterCandidate] = field(default_factory=list)


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
) -> list[EnrichedSource]:
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

    total = len(stats.sources)
    if conn is not None:
        progress(f"Enriching {total} sources with regmeta...")

    t0 = time.monotonic()

    try:
        # Collect all unique column names across all sources
        all_col_names: set[str] = set()
        for source in stats.sources:
            for col in source.columns:
                all_col_names.add(col.column_name)

        # Per-source resolved vars and the register each source votes for
        source_resolved: dict[str, dict[str, _ResolvedVar]] = {}
        source_register: dict[str, int | None] = {}
        source_candidates: dict[str, list[RegisterCandidate]] = {}
        value_codes: dict[int, dict[str, str]] = {}

        if conn is not None:
            if register:
                # Explicit register: single pass, all sources use it
                reg_ids = regmeta.resolve_register_ids(conn, register)
                global_resolved = _bulk_resolve(conn, all_col_names, reg_ids or None)
                for source in stats.sources:
                    source_resolved[source.source_name] = global_resolved
                    source_register[source.source_name] = (
                        reg_ids[0] if reg_ids else None
                    )
            else:
                # Two-pass: vote on register per source, then resolve within it
                col_to_registers = _bulk_resolve_all_registers(conn, all_col_names)

                # Group sources by their voted register so we batch DB queries
                register_to_sources: dict[int | None, list[str]] = {}
                for source in stats.sources:
                    nonid_cols = [
                        c.column_name for c in source.columns if c.inferred_type != "id"
                    ]
                    result = _vote_register(
                        nonid_cols, col_to_registers, source.source_name
                    )
                    source_register[source.source_name] = result.register_id
                    source_candidates[source.source_name] = result.candidates
                    register_to_sources.setdefault(result.register_id, []).append(
                        source.source_name
                    )

                # One _bulk_resolve per distinct voted register
                for reg_id, _names in register_to_sources.items():
                    reg_ids = [reg_id] if reg_id is not None else None
                    resolved = _bulk_resolve(conn, all_col_names, reg_ids)
                    for name in _names:
                        source_resolved[name] = resolved

            # Collect categorical var_ids for value code fetch
            cat_var_ids: set[int] = set()
            for source in stats.sources:
                resolved = source_resolved.get(source.source_name, {})
                for col in source.columns:
                    rv = _lookup_resolved(resolved, col.column_name)
                    if col.inferred_type == "categorical" and rv is not None:
                        cat_var_ids.add(rv.var_id)
            if cat_var_ids:
                value_codes = _bulk_fetch_value_codes(conn, cat_var_ids)

        matched_total = 0
        enriched_sources: list[EnrichedSource] = []
        for source in stats.sources:
            resolved = source_resolved.get(source.source_name, {})
            enriched_cols = []
            for col in source.columns:
                ecol = _column_from_stats(col)
                rv = _lookup_resolved(resolved, ecol.column_name)
                if rv is not None:
                    ecol.register_id = rv.register_id
                    ecol.var_id = rv.var_id
                    ecol.variable_name = rv.variable_name
                    matched_total += 1
                    if ecol.inferred_type == "categorical" and rv.var_id in value_codes:
                        ecol.value_codes = value_codes[rv.var_id]
                enriched_cols.append(ecol)

            enriched_sources.append(
                EnrichedSource(
                    source_name=source.source_name,
                    source_type=source.source_type,
                    source_detail=source.source_detail,
                    row_count=source.row_count,
                    columns=enriched_cols,
                    register_hint=source_register.get(source.source_name),
                    register_hint_candidates=source_candidates.get(
                        source.source_name, []
                    ),
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
        total_cols = sum(len(f.columns) for f in enriched_sources)
        progress(
            f"Enriched {total} sources ({matched_total}/{total_cols} columns matched) "
            f"in {elapsed:.1f}s"
        )
        for w in _check_value_code_drift(enriched_sources):
            progress(f"  Warning: {w}")

    return enriched_sources


def _check_value_code_drift(enriched_sources: list[EnrichedSource]) -> list[str]:
    """Warn when stats contain frequency codes absent from regmeta value codes."""
    warnings: list[str] = []
    for ef in enriched_sources:
        for ec in ef.columns:
            if ec.inferred_type != "categorical" or not ec.value_codes:
                continue
            freq_keys = set(ec.stats.get("frequencies", {})) - {"_other"}
            unknown = sorted(freq_keys - set(ec.value_codes))
            if unknown:
                codes = ", ".join(unknown)
                warnings.append(
                    f"{ef.source_name}/{ec.column_name}: "
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


def _lookup_resolved(
    resolved: dict[str, _ResolvedVar], col_name: str
) -> _ResolvedVar | None:
    """Look up a column by name, falling back to the prefix-stripped form."""
    return resolved.get(col_name.lower()) or resolved.get(
        strip_project_prefix(col_name).lower()
    )


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
# Extend this when new delivery tables with predictable naming are encountered.
_SCB_TABLE_REGISTER: dict[str, int] = {
    "fodelseuppg": 2,  # RTB
    "immigranter": 2,  # RTB
    "population": 2,  # RTB (Population_PersonNr_*)
    "flergen": 349,  # Flergenerationsregistret
}


def _source_name_register_fallback(source_name: str) -> int | None:
    """Match known SCB delivery table names to register IDs by source name stem."""
    stem = source_name.rsplit(".", 1)[0].lower()
    for prefix, reg_id in _SCB_TABLE_REGISTER.items():
        if stem.startswith(prefix):
            return reg_id
    return None


# Minimum fraction of a file's non-id columns that must resolve inside the
# winning register. Below this, the vote is treated as low-confidence and
# register_hint is cleared so downstream tooling asks the user instead of
# confidently mislabeling the file (see GitHub issue #9).
_MIN_MATCH_RATE = 0.40

# Cap the candidate list written to the manifest.
_MAX_CANDIDATES = 5


@dataclass
class _VoteResult:
    register_id: int | None
    candidates: list[RegisterCandidate]


def _vote_register(
    nonid_col_names: list[str],
    col_to_registers: dict[str, list[int]],
    source_name: str = "",
) -> _VoteResult:
    """Pick the best-fit register for a source via weighted majority vote.

    Generic columns (appearing in many registers) are downweighted to avoid
    noise from Kommun/Kön/Ar which exist in 70-120 registers. The winner is
    also required to cover at least ``_MIN_MATCH_RATE`` of the source's
    non-id columns; otherwise the hint is cleared and falls back to known
    SCB delivery table names, then to None. Candidates are returned for
    downstream tooling to present to the user.
    """
    total_nonid = len(nonid_col_names)
    weighted: Counter[int] = Counter()
    match_counts: Counter[int] = Counter()
    for raw_col in nonid_col_names:
        col = strip_project_prefix(raw_col).lower()
        regs = col_to_registers.get(col)
        if not regs:
            continue
        weight = 1.0 / math.log2(max(len(regs), 2))
        for reg_id in regs:
            weighted[reg_id] += weight
            match_counts[reg_id] += 1

    candidates = [
        RegisterCandidate(
            register_id=reg_id,
            match_count=match_counts[reg_id],
            total_nonid_cols=total_nonid,
        )
        for reg_id, _ in sorted(
            match_counts.items(),
            key=lambda kv: (-kv[1], -weighted[kv[0]], kv[0]),
        )
    ][:_MAX_CANDIDATES]

    if not weighted:
        return _VoteResult(_source_name_register_fallback(source_name), candidates)

    top = weighted.most_common(2)
    winner_id, winner_score = top[0]
    if len(top) > 1:
        _, runner_up_score = top[1]
        # Margin guard: sources dominated by generic columns (Kommun, Kön, Ar)
        # produce near-ties. Require a 20% lead OR ≥3 weighted votes
        # (roughly 3+ register-specific columns) before trusting the winner.
        if winner_score < runner_up_score * 1.2 and winner_score < 3:
            return _VoteResult(_source_name_register_fallback(source_name), candidates)

    if total_nonid > 0 and match_counts[winner_id] / total_nonid < _MIN_MATCH_RATE:
        return _VoteResult(_source_name_register_fallback(source_name), candidates)

    return _VoteResult(winner_id, candidates)


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
