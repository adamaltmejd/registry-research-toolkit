"""Enrich stats with regmeta registry metadata."""

from __future__ import annotations

import math
import re
import signal
import sqlite3
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import regmeta
from regmeta.queries import extract_year as _regver_year

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

            # Build per-column requests: (source_name, column_name) ->
            # (var_id, register_id, year, observed_codes, column_name_for_scoring).
            # One source can have two columns resolving to the same (var, reg)
            # (e.g. Individ_2019 with both Sun2000Inr and Sun2020Inr → both →
            # var=784/reg=34) so we cannot share CVID picks per pair — each
            # column needs its own decision. The 5th slot is the
            # project-prefix-stripped form so `_name_score` doesn't tokenize
            # the `P1105_` artifact (which would emit a stray digit token).
            # ``year`` is read from the source's ``source_detail`` (set by
            # ``extract``); ``None`` means "no year hint" and falls through
            # to name/overlap ranking.
            requests: dict[
                tuple[str, str], tuple[int, int, int | None, set[str], str]
            ] = {}
            for source in stats.sources:
                resolved = source_resolved.get(source.source_name, {})
                source_year_raw = source.source_detail.get("year")
                source_year = (
                    int(source_year_raw) if isinstance(source_year_raw, int) else None
                )
                for col in source.columns:
                    rv = _lookup_resolved(resolved, col.column_name)
                    if col.inferred_type != "categorical" or rv is None:
                        continue
                    observed = set(col.stats.get("frequencies", {})) - {"_other"}
                    requests[(source.source_name, col.column_name)] = (
                        rv.var_id,
                        rv.register_id,
                        source_year,
                        observed,
                        strip_project_prefix(col.column_name),
                    )
            value_codes_by_col: dict[tuple[str, str], dict[str, str]] = {}
            if requests:
                value_codes_by_col = _bulk_fetch_value_codes(conn, requests)

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
                    codes = value_codes_by_col.get(
                        (source.source_name, ecol.column_name)
                    )
                    if ecol.inferred_type == "categorical" and codes:
                        ecol.value_codes = codes
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
    """Warn when stats contain frequency codes absent from regmeta value codes.

    Compares on stripped codes and drops whitespace-only/empty observed values:
    SCB tables often pad fixed-width columns (e.g. SsykStatus stores '1 ', '2 ')
    and use blank strings as "no value" sentinels. Neither is a real drift.
    """
    warnings: list[str] = []
    for ef in enriched_sources:
        for ec in ef.columns:
            if ec.inferred_type != "categorical" or not ec.value_codes:
                continue
            freq_keys = set(ec.stats.get("frequencies", {})) - {"_other"}
            valid_stripped = {v.strip() for v in ec.value_codes if v.strip()}
            unknown = sorted(
                k for k in freq_keys if k.strip() and k.strip() not in valid_stripped
            )
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


# Tier-2 (overlap-only) acceptance floor. Below this, an unrelated code
# universe is more likely than a real match — better to leave value_codes
# unset than to mis-enrich with the wrong scheme. See issue #25 (BTYP:
# observed {0..9,B,F,H,L,P} vs CVID {A,E,I,S}: 0% overlap should not pick
# the CVID just because it's the only candidate).
MIN_OVERLAP_RATIO = 0.5

# CamelCase + alpha/digit boundary tokenizer. Four alternatives:
#   1. `[A-Z]+(?=[A-Z][a-z])` — uppercase run before a CamelCase boundary
#      (e.g. `URL` in `URLPath`).
#   2. `[A-Z]+(?![a-z])` — uppercase run not followed by lowercase, i.e.
#      ending the string or followed by a digit / non-letter
#      (e.g. `SSYK` in `SSYK4`, `SUN` in `SUN2000`, `SNI` in `SNI2007`).
#      Without this, all-caps abbreviations vanish from the token set.
#   3. `[A-Z]?[a-z]+` — a normal capitalised or lowercase word.
#   4. `\d+` — a digit run.
# Plain non-alnum splits fail on the flagship case: `Sun2000Inr` would
# stay one token while `SUN2000-INRIKTNING` splits, leaving the
# intersection empty.
_TOKEN_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+")


def _tokenize(s: str) -> list[str]:
    # SCB column names typically strip diacritics (`Kon`, `Fodelseland`)
    # while regmeta labels keep them (`Kön`, `Födelseland`). Fold to
    # NFKD + drop combining marks before matching so the two forms align.
    folded = "".join(
        ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch)
    )
    # Filter to ≥2 char tokens: single letters carry no semantic signal
    # and would spuriously match common column names.
    return [m.lower() for m in _TOKEN_RE.findall(folded) if len(m) >= 2]


def _name_score(col_name: str, *labels: str | None) -> tuple[int, int]:
    """Score a CVID's name labels against a column name.

    Returns ``(shared_tokens, prefix_hits)``. A non-zero pair means the
    CVID is a tier-1 candidate; ``(0, 0)`` falls through to overlap ranking.

    Prefix matching is the secondary signal: it catches Swedish compound
    splits where one side breaks `FamSt` into `["fam", "st"]` and the
    other carries `FamiljeStallningKod` → `["familje", "stallning",
    "kod"]`. Restricting to *prefix* (not free infix) avoids the
    false-positive `"btyp" in "aktivitetstyp"`, which would otherwise
    promote an unrelated CVID to tier-1.
    """
    col_token_set = set(_tokenize(col_name))
    if not col_token_set:
        return (0, 0)
    label_token_set: set[str] = set()
    for lab in labels:
        if lab:
            label_token_set.update(_tokenize(lab))
    shared = len(col_token_set & label_token_set)
    if shared:
        return (shared, 0)
    prefix_hits = sum(
        1
        for ct in col_token_set
        if any(lt.startswith(ct) or ct.startswith(lt) for lt in label_token_set)
    )
    return (0, prefix_hits)


def _year_score(source_year: int | None, cvid_year: int | None) -> tuple[int, int]:
    """Rank a CVID by year proximity.

    Returns ``(known, -distance)``. ``known=1`` only when both years are
    available -- otherwise year is uninformative and the score is
    ``(0, 0)``, falling through to name and overlap ranking. Among
    year-known candidates, exact match (``distance=0``) ranks highest;
    ``distance`` grows linearly with ``|source - cvid|`` so the closer
    year wins among inexact matches.
    """
    if source_year is None or cvid_year is None:
        return (0, 0)
    return (1, -abs(source_year - cvid_year))


def _bulk_fetch_value_codes(
    conn: sqlite3.Connection,
    requests: dict[Any, tuple[int, int, int | None, set[str], str]],
) -> dict[Any, dict[str, str]]:
    """Pick the best CVID for each request, returning {key: {code: label}}.

    Each request is
    ``(var_id, register_id, year, observed_codes, column_name)``.
    We filter CVIDs to the resolved register and rank with a tiered score:

    1. **Year match.** When both the source and the CVID's
       ``register_version.registerversionnamn`` carry a year, prefer the
       CVID whose year is closest (exact > closest > no-info). When
       either side has no year, this tier is neutral and the next tiers
       decide. Year ranks above name because for register-version drift
       (Kommun in 2019 vs 2020), the wrong year's labels are wrong, not
       merely under-precise.
    2. **Name/classification.** Tokenize ``column_name`` and the CVID's
       ``(short_name, vardemangdsversion)`` strings; score by shared token
       count, with prefix containment as fallback. Any non-zero name
       signal accepts the CVID *regardless of code overlap* — name is the
       principled signal for which coding scheme (SUN2000 vs SUN2020).
       Callers must therefore not treat the returned ``value_codes`` as
       a code-set validation; drift between observed codes and the
       picked CVID's universe is surfaced separately by
       ``_check_value_code_drift``.
    3. **Code-set overlap.** Last-resort tiebreak when no CVID has a year
       or name signal. Requires ``overlap / max(len(observed), 1) >=
       MIN_OVERLAP_RATIO`` to accept; otherwise omit the entry. This
       avoids enriching e.g. a 3-digit BTYP column with the dotted
       FamStF code universe.

    The opaque key lets the caller use any identifier — typically
    ``(source_name, column_name)`` — so two columns that resolve to the
    same (var_id, register_id) but with different observed codes can
    still pick different CVIDs.
    """
    if not requests:
        return {}

    # 1. Enumerate CVIDs for every distinct var_id (one query, dedup'd).
    # LEFT JOIN classification so a CVID without classification metadata
    # still appears (with NULL short_name) — overlap can still pick it.
    # JOIN register_version so we can read the version year per CVID for
    # the year-match tier (#24).
    var_ids = sorted({var_id for var_id, _, _, _, _ in requests.values()})
    placeholders = ",".join("?" for _ in var_ids)
    cvid_rows = conn.execute(
        "SELECT vi.var_id, vi.register_id, vi.cvid, "
        "vi.vardemangdsversion, c.short_name AS classification, "
        "rv.registerversionnamn AS regver_name "
        "FROM variable_instance vi "
        "LEFT JOIN classification c ON vi.classification_id = c.id "
        "LEFT JOIN register_version rv ON vi.regver_id = rv.regver_id "
        f"WHERE vi.var_id IN ({placeholders})",
        var_ids,
    ).fetchall()
    pair_to_cvids: dict[tuple[int, int], set[int]] = {}
    cvid_meta: dict[int, tuple[str | None, str | None, int | None]] = {}
    for r in cvid_rows:
        pair_to_cvids.setdefault((r["var_id"], r["register_id"]), set()).add(r["cvid"])
        regver_name = r["regver_name"]
        cvid_year = _regver_year(regver_name) if regver_name else None
        cvid_meta[r["cvid"]] = (
            r["classification"],
            r["vardemangdsversion"],
            cvid_year,
        )

    # 2. Fetch codes for every relevant CVID (one query). Index by cvid.
    all_cvids = sorted({c for cvids in pair_to_cvids.values() for c in cvids})
    if not all_cvids:
        return {}
    placeholders = ",".join("?" for _ in all_cvids)
    value_rows = conn.execute(
        "SELECT vi.cvid, vc.vardekod, vc.vardebenamning "
        "FROM variable_instance vi "
        "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
        "JOIN value_code vc ON vsm.code_id = vc.code_id "
        f"WHERE vi.cvid IN ({placeholders})",
        all_cvids,
    ).fetchall()
    cvid_to_codes: dict[int, dict[str, str]] = {}
    for r in value_rows:
        if r["vardekod"] not in _SCB_TYPE_HINTS:
            cvid_to_codes.setdefault(r["cvid"], {})[r["vardekod"]] = r["vardebenamning"]

    # 3. Per request, score each register-matching CVID and pick the max.
    # Iterate sorted CVIDs so tie-breaks are deterministic (sets are hash-
    # ordered; identical scores would otherwise resolve unpredictably).
    # Score tuple positions are stable: (year_known, -year_distance,
    # shared_tokens, prefix_hits, overlap, len_codes). Earlier tiers
    # dominate in tuple comparison; later fields break ties.
    result: dict[Any, dict[str, str]] = {}
    for key, (
        var_id,
        register_id,
        source_year,
        observed,
        column_name,
    ) in requests.items():
        cvids = sorted(pair_to_cvids.get((var_id, register_id), set()))
        best: tuple[tuple[int, int, int, int, int, int], dict[str, str]] | None = None
        for cvid in cvids:
            codes = cvid_to_codes.get(cvid, {})
            if len(codes) <= 1:
                continue  # a lone code is never a useful categorical universe
            cls_short, vmv, cvid_year = cvid_meta[cvid]
            year_known, year_dist = _year_score(source_year, cvid_year)
            shared, prefix = _name_score(column_name, cls_short, vmv)
            overlap = len(observed & codes.keys()) if observed else 0
            score = (year_known, year_dist, shared, prefix, overlap, len(codes))
            if best is None or score > best[0]:
                best = (score, codes)
        if best is None:
            continue
        score, codes = best
        year_known, _, shared, prefix, overlap, _ = score
        # Tier 1 (year) or Tier 2 (name) accept directly. Year-known with
        # exact or near match expresses real semantic alignment; name
        # match expresses the same for classification schemes.
        if year_known > 0 or shared > 0 or prefix > 0:
            result[key] = codes
            continue
        # Tier 3: no year, no name signal. Require overlap floor when we
        # have observed codes; with no observed codes there is no signal
        # at all, so omit.
        if not observed:
            continue
        ratio = overlap / max(len(observed), 1)
        if ratio >= MIN_OVERLAP_RATIO:
            result[key] = codes

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
