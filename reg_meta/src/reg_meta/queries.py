"""Query functions for reg_meta.

Pure query logic against an open sqlite3.Connection. No CLI concerns
(argument parsing, output formatting, envelopes, timing). These are
the functions that library consumers import.
"""

from __future__ import annotations

import math
import re
from typing import TYPE_CHECKING, Any

from .catalog import Catalog, ConceptGroupMember, GroupFacet
from .errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from .fqid import Fqid, try_emit
from .search import (
    ClassificationSearchResult,
    ClassificationSuccessionSearchResult,
    CodeOwnerClassification,
    CodeOwnerVariable,
    CodeSearchResult,
    ConceptGroupSearchResult,
    DatacolumnSearchResult,
    RegisterSearchResult,
    SearchClassificationEdition,
    SearchResult,
    SearchResults,
    VariableSearchResult,
    VarnameSearchResult,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable

    from .catalog import ConceptGroupSummary

_YEAR_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")

# var_id is the SCB legacy numeric variable id (= SCB's numeric provider_key).
# SOS (provider_key=name) and curated thin providers (provider_key=column) carry
# a non-SCB provider_key, so emit NULL for them (#466) — the display contract is
# "numeric for SCB, blank for everyone else".
#
# Provider gate = the build's minted-id BAND (#474) AND a pure-digit check on
# provider_key — BOTH are needed:
#
#  - The BAND gates out non-SCB providers: every SCB variable_id is `< 2^62`,
#    every non-SCB (SOS, FOHM, curated, steward) variable_id is `>= 2^62`.
#    reg_meta_build/validate.py enforces this (`_check_minted_id_bands`: SCB ids
#    `< _MINT_BIT`, non-SCB ids `>= _MINT_BIT`), so the band is a hard build
#    invariant readable off `variable_id` with no provider join. It catches what
#    the #466 digit heuristic alone could not: a non-SCB provider_key that happens
#    to be digit-only (e.g. a curated column literally named `2020`) is high-band,
#    so it resolves to NULL rather than a bogus `2020`.
#  - The DIGIT check gates out low-band SCB variables whose provider_key is NOT
#    numeric: SCB variable GRAFTS (reg_meta_build/variable_grafts.py) are minted
#    in the SCB band (variable_id < 2^62) but carry a non-numeric provider_key of
#    the form `graft:<column>`. Band-only would `CAST('graft:col' AS INTEGER)` = 0,
#    leaking a bogus `var_id: 0`. The digit check rejects them → NULL. (Restores
#    the #466 behaviour the band-only #474 guard regressed.)
#
# build-invariant: SCB variable_id < 2^62, non-SCB >= 2^62 (band check).
# This literal MIRRORS `reg_meta_build/id.py::_MINT_BIT` (= 1 << 62) — the
# build/runtime boundary keeps `_MINT_BIT` out of reg_meta, so it's duplicated,
# not imported. If `_MINT_BIT` ever moves, update this too;
# `test_band_constant_in_sync_with_build` in
# `reg_meta/tests/test_var_id_nonnumeric.py` asserts equality and will fail CI
# if the two literals diverge.
_SCB_ID_CEILING = 2**62
_VAR_ID_EXPR = (
    "CASE WHEN {vid} < " + str(_SCB_ID_CEILING) + " "
    "AND {pk} GLOB '[0-9]*' AND NOT {pk} GLOB '*[^0-9]*' "
    "THEN CAST({pk} AS INTEGER) ELSE NULL END"
)
# Pre-rendered per qualifier (the variable_id / provider_key column references vary
# by query alias). Plain strings so they splice into the SQL fragments by
# concatenation, no f-string. `vf` is `variable_fts`, whose `rowid` IS the
# variable_id (content-synced rowid alias; see `_search_description_variables`).
_VAR_ID_V = _VAR_ID_EXPR.format(vid="v.variable_id", pk="v.provider_key") + " AS var_id"
_VAR_ID_VF = _VAR_ID_EXPR.format(vid="vf.rowid", pk="vf.provider_key") + " AS var_id"
_VAR_ID_VAR = (
    _VAR_ID_EXPR.format(vid="var.variable_id", pk="var.provider_key") + " AS var_id"
)
_VAR_ID_BARE = _VAR_ID_EXPR.format(vid="variable_id", pk="provider_key") + " AS var_id"


def _try_int(value: str) -> int | str:
    """Convert to int if the string is numeric, otherwise return as-is."""
    try:
        return int(value)
    except ValueError, TypeError:
        return value


# ---------------------------------------------------------------------------
# Register lookup
# ---------------------------------------------------------------------------


def resolve_register_ids(conn: sqlite3.Connection, value: str) -> list[int]:
    """Resolve a register name or ID to a list of register_ids.

    Tries: exact ID → case-insensitive name → substring match.
    Returns empty list if nothing found.
    """
    # IDs are INTEGER — convert for exact match
    row = conn.execute(
        "SELECT register_id FROM register WHERE register_id = ?", (_try_int(value),)
    ).fetchone()
    if row:
        return [row["register_id"]]

    rows = conn.execute(
        # `register.name` is the glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk) of `registernamn`; values
        # are still provider-native (e.g. "LISA").
        "SELECT register_id FROM register WHERE LOWER(name) = LOWER(?)",
        (value,),
    ).fetchall()
    if rows:
        return [r["register_id"] for r in rows]

    rows = conn.execute(
        "SELECT register_id FROM register WHERE LOWER(name) LIKE '%' || LOWER(?) || '%'",
        (value,),
    ).fetchall()
    return [r["register_id"] for r in rows]


def require_register_ids(conn: sqlite3.Connection, value: str) -> list[int]:
    """Like resolve_register_ids but raises NOT_FOUND if empty."""
    ids = resolve_register_ids(conn, value)
    if not ids:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No register matching '{value}'.",
            remediation="Use `reg-meta search` to find valid register names or IDs.",
        )
    return ids


# ---------------------------------------------------------------------------
# Year helpers
# ---------------------------------------------------------------------------


def parse_year_range(spec: str) -> tuple[int | None, int | None]:
    """Parse '2010', '2010-2015', '2010-', '-2015' into (lo, hi) bounds."""
    if "-" in spec:
        parts = spec.split("-", 1)
        lo = int(parts[0]) if parts[0] else None
        hi = int(parts[1]) if parts[1] else None
        return lo, hi
    return int(spec), int(spec)


def extract_year(version_name: str) -> int | None:
    """Extract a 1900-2099 year from a version name. Rejects 4-digit runs
    embedded in longer digit sequences (so "v19999" → None, not 1999) and
    out-of-range numbers (so "Komvux 1234-poäng" → None, not 1234)."""
    m = _YEAR_RE.search(version_name)
    return int(m.group()) if m else None


def _years_in_range(lo_iso: str, hi_iso: str) -> list[int]:
    """A2.6: the calendar years a `variable_state` validity window
    (`valid_from`..`valid_to`, ISO `YYYY-MM-DD`) spans, for DISPLAY enumeration
    only (availability year lists, lineage year ranges). The shipped DB has no
    `register_version` to read an edition year from; the per-state validity
    window is the year source now. The open-ended sentinel `9999-12-31` is
    capped at the start year so a still-active state contributes only its own
    opening year, not a 7000-year run.

    NOT for requested-year FILTERING: capping the open end here would wrongly
    drop a still-active state from any year past its opening. Year filters route
    through `_state_covers_year` / `_state_overlaps_years` instead, which read
    the sentinels with `<=`/`>=` and keep open-ended/multi-year windows."""
    lo = int(lo_iso[:4])
    hi = int(hi_iso[:4])
    if hi >= 9999:
        return [lo]
    return list(range(lo, hi + 1))


def _state_covers_year(valid_from: str, valid_to: str, year: int) -> bool:
    """True when a `variable_state` validity window (`valid_from`..`valid_to`,
    ISO `YYYY-MM-DD`) covers the calendar `year`.

    A2.6 overlap semantics for requested-year FILTERS: a window with year bounds
    `[from_year, to_year]` covers `year` iff `from_year <= year <= to_year`. The
    `9999` (open-ended) and `0001` (yearless-fallback) sentinels read naturally
    under `<=`/`>=`, so a multi-year, still-active, or yearless window matches
    any year it actually spans — not just its opening year."""
    return int(valid_from[:4]) <= year <= int(valid_to[:4])


def _state_overlaps_years(
    valid_from: str, valid_to: str, lo: int | None, hi: int | None
) -> bool:
    """True when a `variable_state` validity window overlaps the requested year
    range `[lo, hi]` (either bound may be ``None`` for open-ended).

    A2.6 overlap semantics: window `[from_year, to_year]` overlaps `[lo, hi]` iff
    `from_year <= hi AND to_year >= lo`. Missing bounds widen to the sentinels
    (`hi=None` → 9999, `lo=None` → 0) so an open-ended request matches every
    window, and the `9999`/`0001` window sentinels match correctly too."""
    from_year = int(valid_from[:4])
    to_year = int(valid_to[:4])
    return from_year <= (hi if hi is not None else 9999) and to_year >= (
        lo if lo is not None else 0
    )


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


SEARCH_FIELDS = frozenset({"datacolumn", "varname", "description", "value", "all"})
# `register`/`variable` partition the two FTS-backed leaf surfaces; `classification`
# (#350) covers the third shipped FTS index (`classification_fts`), previously built
# but unsearched (see DESIGN.md → FTS5 configuration). `value` (#352) is the
# code/value surface (`value_code_fts` + code-shape match), emitting `type: "code"`
# rows annotated with owning variables/classifications. `all` spans every type.
SEARCH_TYPES = frozenset({"register", "variable", "classification", "value", "all"})

# A "real" FTS token carries at least one unicode alphanumeric char; pure
# punctuation tokenizes to nothing in unicode61 and would yield an empty phrase.
_FTS_WORD_CHAR = re.compile(r"[^\W_]", re.UNICODE)


def _fts_match_query(raw: str) -> str | None:
    """Build a safe FTS5 MATCH expression from a raw user query.

    Each whitespace token becomes a quoted prefix term (``"tok"*``): quoting
    neutralizes every FTS5 operator (AND/OR/NOT/NEAR/-/:/^/parens/quotes) so
    stray or hostile syntax can neither raise a ``SyntaxError`` nor change
    semantics; the trailing ``*`` makes it a prefix match ("ink" → "inkomst").
    Tokens are space-joined (implicit AND). Embedded double quotes are doubled
    per FTS5 string-literal escaping. Diacritics are NOT folded here — unicode61
    folds both the index AND query side (å→a), so a Python fold would be
    redundant (and would double-fold). Returns None when no token carries an
    alphanumeric char (empty / whitespace / punctuation-only)."""
    terms = [
        f'"{tok.replace(chr(34), chr(34) * 2)}"*'
        for tok in raw.split()
        if _FTS_WORD_CHAR.search(tok)
    ]
    return " ".join(terms) if terms else None


def _escape_like(s: str) -> str:
    """Escape SQL LIKE metacharacters so user text matches literally.

    Escape the escape char first.
    """
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _filter_search_by_years(
    conn: sqlite3.Connection,
    results: list[dict[str, Any]],
    years: str,
) -> list[dict[str, Any]]:
    """Filter search results to those with versions in the given year range."""
    year_lo, year_hi = parse_year_range(years)
    if not results:
        return results

    # Collect the variable hits to year-filter at VARIABLE granularity and the
    # register hits to filter register-wide (#474). A variable hit carries its
    # unique `_variable_id`; the displayed `var_id` is NULL for every non-SCB
    # variable, so keying on it folded every non-SCB hit into the register-level
    # branch (kept if ANY sibling overlapped). `_variable_id` filters each hit by
    # its OWN states. Non-variable hits (register / classification / code) carry
    # no `_variable_id`.
    var_ids_to_check: set[int] = set()
    reg_only_ids: set[int] = set()
    for r in results:
        rid = r.get("register_id")
        variable_id = r.get("_variable_id")
        if variable_id is not None:
            var_ids_to_check.add(variable_id)
        elif rid is not None:
            reg_only_ids.add(rid)

    # A2.6: edition years come from `variable_state` validity windows now (the
    # register_version table is dropped before ship). A variable is in-range if
    # any of its states' validity window overlaps the year range.
    valid_var_ids: set[int] = set()
    if var_ids_to_check:
        placeholders = ",".join("?" * len(var_ids_to_check))
        rows = conn.execute(
            "SELECT DISTINCT vs.variable_id, vs.valid_from, vs.valid_to "
            "FROM variable_state vs "
            f"WHERE vs.variable_id IN ({placeholders})",
            list(var_ids_to_check),
        ).fetchall()
        for row in rows:
            if _state_overlaps_years(
                row["valid_from"], row["valid_to"], year_lo, year_hi
            ):
                valid_var_ids.add(row["variable_id"])

    # For register-type results: check if register has any state in range.
    valid_reg_ids: set[int] = set()
    if reg_only_ids:
        placeholders = ",".join("?" * len(reg_only_ids))
        rows = conn.execute(
            "SELECT DISTINCT v.register_id, vs.valid_from, vs.valid_to "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            f"WHERE v.register_id IN ({placeholders})",
            list(reg_only_ids),
        ).fetchall()
        for row in rows:
            if _state_overlaps_years(
                row["valid_from"], row["valid_to"], year_lo, year_hi
            ):
                valid_reg_ids.add(row["register_id"])

    filtered = []
    for r in results:
        rid = r.get("register_id")
        variable_id = r.get("_variable_id")
        if variable_id is not None:
            # Variable hit — filtered by its OWN states (#474), not register-wide.
            if variable_id in valid_var_ids:
                filtered.append(r)
        elif rid is not None:
            if rid in valid_reg_ids:
                filtered.append(r)
        elif r.get("type") == "classification":
            # Classifications carry no register/state validity window (#350), so a
            # --years filter (a version/validity filter) can't confirm them in
            # range — exclude rather than return as unfilterable false positives
            # (Codex P2). The vintage lives in the slug, not a comparable column;
            # vintage-year filtering is future work.
            continue
        else:
            filtered.append(r)
    return filtered


def search(
    conn: sqlite3.Connection,
    query: str,
    *,
    field: str = "all",
    type: str = "all",
    register: str | None = None,
    years: str | None = None,
    limit: int = 50,
    offset: int = 0,
    fold_groups: bool = True,
) -> SearchResults:
    """Search across registers, variables, and classifications.

    field controls what is searched:
      - "datacolumn": column aliases (LIKE pattern match)
      - "varname": canonical variable names (LIKE pattern match)
      - "description": FTS over the shipped indexes — register name/purpose,
        variable name/definition/description, and classification
        short_name/name/name_en/description (#350)
      - "value": value labels via `value_code_fts` (FTS) + exact/prefix match on
        `value_code.code` for code-shaped queries; use with `type="value"` (#352)
      - "all": all of the above (default)

    type filters which entity surfaces are returned ("register" / "variable" /
    "classification" / "value" / "all"). Classifications are catalog-scoped, so a
    `register` scope excludes them. Each register/variable/classification leaf row
    carries its navigable `fqid` (None when the entity isn't slugged). `value`
    (#352) returns `type: "code"` rows — each a (code, label) hit annotated with
    its owning variables/classifications (a bounded representative slice under
    `variables`/`classifications` plus the full `variable_count`/
    `classification_count`); the owning entity, not the bare code pair, is the
    actionable target.

    fold_groups (#322): when hits land on ≥2 member variables of one concept
    group (see DESIGN.md → Concept groups), the sibling hits collapse into a
    single `type: "group"` result row (original hits under `matched`, the full
    member list under `members`); a lone member hit stays a leaf row annotated
    with `concept_group`/`concept_group_label`. Group LABELS match too — for
    `field` in ("varname", "description", "all"), a query matching a group's
    label/key emits the group row even when no leaf row matches. Result-shaping
    only; folding happens before pagination, so a group row counts as one result.

    Returns a `SearchResults` (`total_count` + the sliced, folded `results` tuple
    of typed result models, discriminated on `type`). Doc results are NOT included
    here — the CLI layer merges them separately.
    """
    if field not in SEARCH_FIELDS:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message=f"Invalid search field '{field}'. Valid: {sorted(SEARCH_FIELDS)}",
            remediation="Use --datacolumn, --varname, --description, --value, or --all-fields.",
        )
    if type not in SEARCH_TYPES:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message=f"Invalid search type '{type}'. Valid: {sorted(SEARCH_TYPES)}",
            remediation="Use --type register, variable, classification, value, or all.",
        )

    reg_ids: set[int] | None = None
    if register:
        ids = resolve_register_ids(conn, register)
        if not ids:
            return SearchResults(total_count=0, results=())
        reg_ids = set(ids)

    _REGISTER_TYPES = {"register"}
    _VARIABLE_TYPES = {"variable", "varname", "datacolumn"}
    _CLASSIFICATION_TYPES = {"classification"}
    _VALUE_TYPES = {"code"}

    all_results: list[dict[str, Any]] = []
    like_pattern = f"%{_escape_like(query)}%"
    # The FTS path (register/variable/classification indexes) takes a SAFE FTS5
    # MATCH expression built from the raw query — quoted prefix terms that
    # neutralize FTS operators and won't error on stray syntax (see
    # `_fts_match_query`). LIKE paths use an escaped substring pattern so user
    # `%` / `_` input stays literal. None = the query had no usable token, so the
    # FTS indexes contribute nothing.
    fts_query = _fts_match_query(query)

    # Classifications surfaced by the name-FTS arm; the code-containment arm
    # (#393 item 5) excludes them so a both-ways match isn't emitted twice.
    classification_name_ids: set[int] = set()

    if field in ("datacolumn", "all"):
        all_results.extend(_search_datacolumns(conn, like_pattern, reg_ids))

    if field in ("varname", "all"):
        all_results.extend(_search_varnames(conn, like_pattern, reg_ids))

    if field in ("description", "all") and fts_query is not None:
        if type in ("register", "all"):
            all_results.extend(_search_description_registers(conn, fts_query, reg_ids))
        if type in ("variable", "all"):
            all_results.extend(_search_description_variables(conn, fts_query, reg_ids))
        # Classifications are catalog-scoped (no register), so a `--register` scope
        # excludes them — `reg_ids` set means "registers only".
        if type in ("classification", "all") and reg_ids is None:
            cls_rows = _search_classifications(conn, fts_query)
            classification_name_ids = {r["_classification_id"] for r in cls_rows}
            all_results.extend(cls_rows)

    # Code-aware classification surfacing (#393 item 5): a code-shaped query also
    # surfaces the classifications that CONTAIN a matching code (C12 -> ICD-10-SE),
    # so "find the classification for this code" works even with no NAME match.
    # SEPARATE top-level block (NOT under the `fts_query is not None` gate): it
    # matches the RAW code against `value_code.code`, not the FTS index. Ranked
    # AFTER the name-FTS hits (positive vs negative `fts_rank`) and catalog-scoped
    # — excluded under a `--register` scope, exactly like the name-FTS arm.
    if (
        field in ("description", "all")
        and type in ("classification", "all")
        and reg_ids is None
        and _is_code_shaped(query)
    ):
        all_results.extend(
            _search_classifications_by_code(conn, query, classification_name_ids)
        )

    # Code/value search (#352): FTS over value_code labels + exact/prefix code
    # match, annotated with owning variables / classifications. Emits `type:
    # "code"` rows. Gated on `value`/`all` field AND a non-`register`-only type
    # scope (a `register` type request wants register leaves, not codes). Returns
    # the FULL in-scope match set (like the other arms); the outer offset/limit
    # slice below is what paginates, so total_count is the true count.
    if field in ("value", "all") and type in ("value", "all"):
        all_results.extend(_search_values_fts(conn, query, reg_ids))

    if type == "register":
        all_results = [r for r in all_results if r["type"] in _REGISTER_TYPES]
    elif type == "variable":
        all_results = [r for r in all_results if r["type"] in _VARIABLE_TYPES]
    elif type == "classification":
        all_results = [r for r in all_results if r["type"] in _CLASSIFICATION_TYPES]
    elif type == "value":
        all_results = [r for r in all_results if r["type"] in _VALUE_TYPES]

    if years:
        all_results = _filter_search_by_years(conn, all_results, years)

    if fold_groups:
        # Label hits ride the NAME surface (a group label is a concept name).
        # `varname`/`all` search names directly; `description` (#350, the
        # FTS-index field driving /api/search) folds by concept too, so a query
        # matching a family LABEL but no member's FTS text still surfaces the
        # group — `_search_group_labels` is the only path that finds it. A group
        # has no validity window of its own, so --years applies through its
        # MEMBERS: a variable-kind label hit needs at least one member state
        # overlapping the range (member hits were already year-filtered above;
        # this guards the label-only path). Classification groups are exempt —
        # their members carry no delivery windows.
        # `fts_query is not None` == the query has a real searchable token. Gate
        # label folding on it so an empty / punctuation-only query doesn't turn
        # the raw `%%` LIKE pattern into a match-every-group (Codex P2) — the FTS
        # leaf path already contributes nothing in that case.
        label_hits = (
            _search_group_labels(
                conn,
                like_pattern,
                reg_ids,
                type=type,
                year_range=parse_year_range(years) if years else None,
            )
            if field in ("varname", "description", "all") and fts_query is not None
            else []
        )
        # Collapse classification EDITION chains FIRST (#571): editions aren't
        # concept_group members, so they need a separate fold. Once collapsed to
        # their terminal, the curated SUN-style umbrella group (#516) then folds
        # the terminals into `group:sun` cleanly below.
        all_results = _fold_classification_succession(conn, all_results)
        all_results = _fold_concept_groups(conn, all_results, label_hits)

    all_results.sort(key=lambda x: x.get("fts_rank", 0))
    total_count = len(all_results)
    results = all_results[offset : offset + limit]

    # Annotate value/code rows AFTER the slice: the unscoped value arm
    # (`reg_ids is None`) returns rows unannotated with a `_code_id` marker, so we
    # only run the owner-annotation queries for the ≤limit codes actually shown
    # (the omnibox-timeout fix for broad terms). No-op for the reg-scoped arm
    # (those rows are already annotated and carry no `_code_id`).
    _annotate_value_page(conn, results, reg_ids)
    # Strip fold-internal keys from the SHOWN page only — non-page rows are
    # discarded, so there's nothing to clean on them. (`_strip_internal_keys`
    # touches only `_INTERNAL_KEYS`, never `fts_rank`, so the sort above is safe.)
    _strip_internal_keys(results)

    # Convert the internal dict rows to the typed result models ONCE, at the end
    # (#701): the dict pipeline above is unchanged; this is the single read/write
    # boundary where the heterogeneous rows become the discriminated `SearchResult`
    # union. `fts_rank` → the public `rank`.
    return SearchResults(
        total_count=total_count,
        results=tuple(_row_to_model(r) for r in results),
    )


def _matched_count(row: dict[str, Any]) -> int:
    """How many leaf hits a fold row (`group` / `classification_succession`)
    collapsed — the length of its `matched` list."""
    return len(row.get("matched") or [])


def _member_models(row: dict[str, Any]) -> tuple[ConceptGroupMember, ...]:
    """Rehydrate a fold row's `members` dicts into `ConceptGroupMember`s (the
    `fqid` is a string the `Fqid` field parses; `facets` are `{axis, value, label}`
    dicts)."""
    return tuple(
        ConceptGroupMember(
            fqid=m["fqid"],
            name=m.get("name"),
            facets=tuple(GroupFacet(**f) for f in m.get("facets", [])),
        )
        for m in row.get("members", [])
    )


def _code_system(classifications: list[dict[str, Any]]) -> str | None:
    """The code system a code belongs to (#393 item 3): the primary/first owning
    classification's `short_name` (fall back to `name`). None for register-local /
    bespoke codes with no owning classification."""
    if not classifications:
        return None
    first = classifications[0]
    return first.get("short_name") or first.get("name")


def _row_to_model(row: dict[str, Any]) -> SearchResult:
    """Map one internal search-result dict to its typed model, dispatching on
    `row["type"]` (#701). `fts_rank` → `rank`; nested members / editions / owners
    are rehydrated into their models. This is the ONLY place the dict-pipeline rows
    cross into the typed contract — the arms upstream stay dict-based."""
    row_type = row["type"]
    rank = row.get("fts_rank", 0)
    if row_type == "register":
        return RegisterSearchResult(
            fqid=row.get("fqid"),
            name=row.get("register_name"),
            purpose=row.get("register_purpose"),
            rank=rank,
        )
    if row_type == "variable":
        return VariableSearchResult(
            fqid=row.get("fqid"),
            name=row.get("variable_name"),
            register=row.get("register_name"),
            definition=row.get("variable_definition"),
            concept_group=row.get("concept_group"),
            concept_group_label=row.get("concept_group_label"),
            rank=rank,
        )
    if row_type == "classification":
        return ClassificationSearchResult(
            fqid=row.get("fqid"),
            short_name=row.get("short_name"),
            name=row.get("classification_name"),
            concept_group=row.get("concept_group"),
            concept_group_label=row.get("concept_group_label"),
            terminal_fqid=row.get("terminal_fqid"),
            rank=rank,
        )
    if row_type == "classification_succession":
        return ClassificationSuccessionSearchResult(
            fqid=row.get("fqid"),
            short_name=row.get("short_name"),
            name=row.get("classification_name"),
            editions=tuple(
                SearchClassificationEdition(
                    slug=e["slug"],
                    fqid=e.get("fqid"),
                    name=e.get("name"),
                    effective_year=e.get("effective_year"),
                )
                for e in row.get("editions", [])
            ),
            matched_count=_matched_count(row),
            rank=rank,
        )
    if row_type == "group":
        return ConceptGroupSearchResult(
            kind=row["kind"],
            group_key=row["group_key"],
            group_label=row["group_label"],
            source=row.get("group_source"),
            register=row.get("register_name"),
            member_count=row.get("member_count", 0),
            matched_count=_matched_count(row),
            label_matched=row.get("label_matched", False),
            members=_member_models(row),
            rank=rank,
        )
    if row_type == "code":
        classifications = row.get("classifications", [])
        return CodeSearchResult(
            code=row["code"],
            label=row["label"],
            variables=tuple(
                CodeOwnerVariable(
                    fqid=v.get("fqid"),
                    name=v.get("name"),
                    register=v.get("register"),
                )
                for v in row.get("variables", [])
            ),
            variable_count=row.get("variable_count", 0),
            classifications=tuple(
                CodeOwnerClassification(
                    fqid=c.get("fqid"),
                    short_name=c.get("short_name"),
                    name=c.get("name"),
                )
                for c in classifications
            ),
            classification_count=row.get("classification_count", 0),
            code_system=_code_system(classifications),
            rank=rank,
        )
    if row_type == "datacolumn":
        return DatacolumnSearchResult(
            datacolumn=row["datacolumn"],
            register=row.get("register_name"),
            var_id=row.get("var_id"),
            name=row.get("variable_name"),
            concept_group=row.get("concept_group"),
            concept_group_label=row.get("concept_group_label"),
            rank=rank,
        )
    if row_type == "varname":
        return VarnameSearchResult(
            register=row.get("register_name"),
            var_id=row.get("var_id"),
            name=row.get("variable_name"),
            concept_group=row.get("concept_group"),
            concept_group_label=row.get("concept_group_label"),
            rank=rank,
        )
    raise RegMetaError(
        exit_code=EXIT_NOT_FOUND,
        code="unknown_search_row_type",
        error_class="query",
        message=f"search produced a row with unknown type {row_type!r}",
        remediation="This is an internal invariant break; please report it.",
    )


def _search_datacolumns(
    conn: sqlite3.Connection, like_pattern: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    # Aliased SELECT so both `variable.name` and `register.name` land under
    # distinct row keys after the glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk) collapsed them to a single
    # column name.
    # A2.7: `variable_alias` is variable_id-keyed now (was cvid-keyed). Join
    # straight to `variable` via `variable_id`; `var_id` is the variable's
    # `provider_key`.
    rows = conn.execute(
        "SELECT DISTINCT va.delivery_column_name, v.register_id, v.variable_id, "
        "" + _VAR_ID_V + ", "
        "v.name AS variable_name, r.name AS register_name "
        "FROM variable_alias va "
        "JOIN variable v ON va.variable_id = v.variable_id "
        "JOIN register r ON v.register_id = r.register_id "
        "WHERE va.delivery_column_name LIKE ? ESCAPE '\\' "
        "ORDER BY va.delivery_column_name, v.register_id",
        (like_pattern,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "datacolumn",
                "datacolumn": r["delivery_column_name"],
                "register_id": r["register_id"],
                "register_name": r["register_name"],
                "var_id": r["var_id"],
                "variable_name": r["variable_name"],
                "fts_rank": 0,
                "_variable_id": r["variable_id"],
            }
        )
    return results


def _search_varnames(
    conn: sqlite3.Connection, like_pattern: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT v.register_id, v.variable_id, "
        "" + _VAR_ID_V + ", "
        "v.name AS variable_name, r.name AS register_name "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "WHERE v.name LIKE ? ESCAPE '\\' "
        "ORDER BY v.name, v.register_id",
        (like_pattern,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "varname",
                "register_id": r["register_id"],
                "register_name": r["register_name"],
                "var_id": r["var_id"],
                "variable_name": r["variable_name"],
                "fts_rank": 0,
                "_variable_id": r["variable_id"],
            }
        )
    return results


def _search_description_registers(
    conn: sqlite3.Connection, query: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    # register_fts now mirrors the renamed columns: `name` + `purpose`.
    # `registerrubrik` was dropped per the glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk).
    # Join `register`/`provider` for the slugs so each hit carries its 2-seg
    # `fqid` — the navigation key discovery surfaces (#350 /api/search) need and
    # the flat search row otherwise lacks. `try_emit` yields None for an
    # unslugged register (not catalog-addressable); additive for CLI consumers.
    rows = conn.execute(
        "SELECT rf.register_id, rf.name, rf.purpose, rf.rank, "
        "r.slug AS register_slug, p.slug AS provider_slug "
        "FROM register_fts rf "
        "JOIN register r ON r.register_id = rf.register_id "
        "JOIN provider p ON p.provider_id = r.provider_id "
        "WHERE register_fts MATCH ? "
        "ORDER BY rf.rank",
        (query,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "register",
                "fqid": try_emit(
                    Fqid.register_fqid, r["provider_slug"], r["register_slug"]
                ),
                "register_id": r["register_id"],
                "register_name": r["name"],
                "register_purpose": r["purpose"],
                "fts_rank": r["rank"],
            }
        )
    return results


def _search_description_variables(
    conn: sqlite3.Connection, query: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    # `variable_fts` is content-synced to `variable` (content_rowid='rowid', and
    # `variable_id` is the INTEGER PRIMARY KEY rowid alias), so `vf.rowid` IS
    # the variable_id — carried for concept-group folding (#322).
    # Join `variable`/`provider` for the slugs so each hit carries its 3-seg
    # binding `fqid` (#350) — the navigation key /api/search needs. `v.slug` /
    # `p.slug` feed `try_emit`, which yields None for an unslugged variable.
    rows = conn.execute(
        "SELECT vf.register_id, vf.rowid AS variable_id, "
        "" + _VAR_ID_VF + ", "
        "vf.name AS variable_name, vf.definition AS variable_definition, "
        "vf.description AS variable_description, vf.rank, "
        "r.name AS register_name, r.purpose AS register_purpose, "
        "r.slug AS register_slug, p.slug AS provider_slug, v.slug AS variable_slug "
        "FROM variable_fts vf "
        "JOIN register r ON vf.register_id = r.register_id "
        "JOIN provider p ON p.provider_id = r.provider_id "
        "JOIN variable v ON v.variable_id = vf.rowid "
        "WHERE variable_fts MATCH ? "
        "ORDER BY vf.rank",
        (query,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "variable",
                "fqid": try_emit(
                    Fqid.binding_fqid,
                    r["provider_slug"],
                    r["register_slug"],
                    r["variable_slug"],
                ),
                "register_id": r["register_id"],
                "register_name": r["register_name"],
                "register_purpose": r["register_purpose"],
                "var_id": r["var_id"],
                "variable_name": r["variable_name"],
                "variable_definition": r["variable_definition"],
                "fts_rank": r["rank"],
                "_variable_id": r["variable_id"],
            }
        )
    return results


# How many owning variables / classifications a single code hit carries on the
# wire. A common code maps to thousands of variables (`code_variable_map` is
# 4.1M rows); the result row a researcher wants is the variable or classification
# carrying the code, so each code hit surfaces a bounded representative slice plus
# the full count (the SPA shows "+N more"). Owners are ordered by the variable's
# own discriminativeness — variables that carry FEWER distinct codes first (a code
# on a 3-value enum is more telling than the same code on a 500-value catalog).
_CODE_OWNERS_PER_HIT = 5

# A code-shaped query gets an exact + prefix match on `value_code.code`
# (idx_value_code_code) merged with the label-FTS hits: a digit AND length >= 3
# ("F32", "0180", "47.11"). Pure text queries do label FTS only — 55% of codes
# are bare numbers, so unconditional code matching is noise.
_CODE_SHAPED_RE = re.compile(r"\d")


def _is_code_shaped(query: str) -> bool:
    q = query.strip()
    return len(q) >= 3 and _CODE_SHAPED_RE.search(q) is not None


def _empty_owner_annotation() -> dict[str, Any]:
    return {
        "variables": [],
        "variable_count": 0,
        "classifications": [],
        "classification_count": 0,
    }


def _code_owner_annotations_batch(
    conn: sqlite3.Connection, code_ids: list[int], reg_ids: set[int] | None
) -> dict[int, dict[str, Any]]:
    """Resolve owning variables / classifications for a SET of codes at once (#352).

    Set-based, NOT per-code: a single label token can match thousands of codes, so
    a per-hit count+slice query (the old `_code_owner_annotations`) would be an N+1
    blowup. We materialize the matched `code_id`s into a TEMP table and JOIN
    `code_variable_map` / `classification_code` against it — JOIN (not
    `WHERE code_id IN (<thousands>)`, which risks SQLite's bound-param limit). Owner
    slices are capped to `_CODE_OWNERS_PER_HIT` per code via a window
    `ROW_NUMBER()`; the counts are the FULL per-code totals.

    Semantics preserved from the per-code version:
      - variables via `code_variable_map` (variable_id-grained, so a split sibling
        is attributed only the codes its own value set carried — no fan-out);
      - owner ordering by the variable's own distinct-code count ASC (a code on a
        tight enum is more discriminative than the same code on a 500-value
        catalog), ties broken by slug for determinism;
      - `reg_ids` (a `--register` scope) constrains BOTH the variable owners and
        `variable_count`; classifications are catalog-scoped, so a register scope
        leaves them empty (mirrors `_search_classifications`' guard)."""
    out: dict[int, dict[str, Any]] = {
        cid: _empty_owner_annotation() for cid in code_ids
    }
    if not code_ids:
        return out

    # TEMP table of the matched code_ids — the JOIN driver. Dropped on connection
    # close; we also DROP explicitly so repeated search() calls on a long-lived
    # connection (the webapp's per-request conn) don't collide.
    conn.execute("DROP TABLE IF EXISTS _match_code_ids")
    conn.execute("CREATE TEMP TABLE _match_code_ids (code_id INTEGER PRIMARY KEY)")
    conn.executemany(
        "INSERT INTO _match_code_ids (code_id) VALUES (?)",
        [(cid,) for cid in code_ids],
    )
    try:
        reg_filter = ""
        reg_params: list[Any] = []
        if reg_ids:
            reg_filter = " AND v.register_id IN (" + ",".join("?" * len(reg_ids)) + ")"
            reg_params = sorted(reg_ids)

        # Full per-code variable count (register-scoped when reg_ids set).
        for row in conn.execute(
            "SELECT cvm.code_id, COUNT(*) AS n "
            "FROM _match_code_ids m "
            "JOIN code_variable_map cvm ON cvm.code_id = m.code_id "
            "JOIN variable v ON cvm.variable_id = v.variable_id "
            f"WHERE 1=1{reg_filter} "
            "GROUP BY cvm.code_id",
            reg_params,
        ):
            out[row["code_id"]]["variable_count"] = row["n"]

        # Top-N variable owners per code via a windowed rank over the same ordering
        # the per-code slice used (var_code_count ASC, slug). The outer filter on
        # rn keeps the cap; provider/register/variable slugs feed the binding FQID.
        var_rows = conn.execute(
            "WITH owners AS ("
            "  SELECT cvm.code_id, v.name AS variable_name, v.slug AS variable_slug, "
            "         r.name AS register_name, r.slug AS register_slug, "
            "         p.slug AS provider_slug, "
            "         (SELECT COUNT(*) FROM code_variable_map c2 "
            "            WHERE c2.variable_id = v.variable_id) AS var_code_count "
            "  FROM _match_code_ids m "
            "  JOIN code_variable_map cvm ON cvm.code_id = m.code_id "
            "  JOIN variable v ON cvm.variable_id = v.variable_id "
            "  JOIN register r ON v.register_id = r.register_id "
            "  JOIN provider p ON p.provider_id = r.provider_id "
            f"  WHERE 1=1{reg_filter}"
            "), ranked AS ("
            "  SELECT *, ROW_NUMBER() OVER ("
            "    PARTITION BY code_id ORDER BY var_code_count ASC, variable_slug"
            "  ) AS rn FROM owners"
            ") SELECT * FROM ranked WHERE rn <= ? ORDER BY code_id, rn",
            (*reg_params, _CODE_OWNERS_PER_HIT),
        ).fetchall()
        for r in var_rows:
            out[r["code_id"]]["variables"].append(
                {
                    "fqid": try_emit(
                        Fqid.binding_fqid,
                        r["provider_slug"],
                        r["register_slug"],
                        r["variable_slug"],
                    ),
                    "name": r["variable_name"],
                    "register": r["register_name"],
                }
            )

        # Classifications: catalog-scoped, so a register scope leaves them empty.
        # This owner definition (variables ∪ classifications, with NO is_valid/validity
        # filter on classification_code) is MIRRORED at build time by the value_code_fts
        # owner filter in reg_meta_build/db.py `_populate_fts` (#478). Any change to what
        # counts as a classification owner here (e.g. adding an is_valid predicate) MUST
        # be mirrored there, or the search index and the owner annotation desync —
        # context-less hits leak into search, or valid classification codes vanish.
        if not reg_ids:
            for row in conn.execute(
                "SELECT cc.code_id, COUNT(*) AS n "
                "FROM _match_code_ids m "
                "JOIN classification_code cc ON cc.code_id = m.code_id "
                "GROUP BY cc.code_id"
            ):
                out[row["code_id"]]["classification_count"] = row["n"]
            cls_rows = conn.execute(
                "WITH owners AS ("
                "  SELECT cc.code_id, c.short_name, c.name, c.slug "
                "  FROM _match_code_ids m "
                "  JOIN classification_code cc ON cc.code_id = m.code_id "
                "  JOIN classification c ON c.id = cc.classification_id "
                "), ranked AS ("
                "  SELECT *, ROW_NUMBER() OVER ("
                "    PARTITION BY code_id ORDER BY short_name"
                "  ) AS rn FROM owners"
                ") SELECT * FROM ranked WHERE rn <= ? ORDER BY code_id, rn",
                (_CODE_OWNERS_PER_HIT,),
            ).fetchall()
            for r in cls_rows:
                out[r["code_id"]]["classifications"].append(
                    {
                        "fqid": try_emit(Fqid.classification_fqid, r["slug"]),
                        "short_name": r["short_name"],
                        "name": r["name"],
                    }
                )
    finally:
        conn.execute("DROP TABLE IF EXISTS _match_code_ids")

    return out


def _search_values_fts(
    conn: sqlite3.Connection, query: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    """Code/value search over `value_code_fts` (#352).

    Returns the FULL ranked, in-scope match set (NOT internally limited or offset),
    exactly like the register/variable/classification arms — `search()` does the
    `total_count = len(...)` + `[offset:offset+limit]` slice, so total_count is the
    true match count and offset paginates. (An earlier draft truncated to `limit*4`
    + `[:limit]` here, which false-emptied register-scoped queries whose top codes
    were all out-of-scope, saturated total_count at `limit`, and broke offset.)

    Label FTS (bm25) is the primary surface — ~55% of codes are bare numbers, so
    labels carry the meaning. Each label hit JOINs `value_code` for the (code,
    label, mapping_count). For a code-shaped query (`_is_code_shaped`) an exact +
    prefix match on `value_code.code` is merged in (deduped by code_id) — labels
    alone miss a researcher entering "F32".

    Owner annotation (owning variables / classifications) splits on scope:
      - `reg_ids is None` (the webapp path): annotation is DEFERRED to the shown
        page. Rows come back unannotated (empty owners) with an internal
        `_code_id` marker; `search()` annotates only the ≤limit page rows via
        `_annotate_value_page`. A broad term matches thousands of codes, so
        annotating the full set before the offset/limit slice was the omnibox
        timeout this avoids.
      - `reg_ids is not None` (`--register`): the full set is annotated up front
        (`_code_owner_annotations_batch`, set-based to avoid an N+1) so the
        reg-scope drop can run; the match set is small (one register's codes).

    Ranking: bm25 relevance, then `mapping_count` ASCENDING — a label shared by
    many variables (a generic enum) is less discriminative than a rare one, so it
    ranks LOWER. We expose a combined `fts_rank` where a smaller value sorts
    first (matching the other FTS searches): bm25 is already smaller-is-better, and
    a tiny mapping_count tie-break term keeps the order deterministic without
    letting common-but-relevant labels (Småort) drop out entirely. Code-shaped
    exact/prefix hits are seeded ABOVE all label hits (rank below the FTS floor),
    since an exact code match is the strongest signal a code query can get."""
    fts_query = _fts_match_query(query)

    # code_id → (code, label, mapping_count, base_rank). base_rank is the sort key
    # (smaller first). Dedup by code_id so a code matched both by label-FTS and by
    # code-shape collapses to one hit (the stronger/lower rank wins).
    hits: dict[int, dict[str, Any]] = {}

    def _hit(r: sqlite3.Row, base_rank: float) -> dict[str, Any]:
        return {
            "code_id": r["code_id"],
            "code": r["code"],
            "label": r["label"],
            "mapping_count": r["mapping_count"],
            "base_rank": base_rank,
        }

    if fts_query is not None:
        # bm25 default weights; mapping_count downweight is a small additive term
        # (scaled by log so a 60k-mapping junk-ish label sinks but doesn't dwarf
        # bm25). Both terms are smaller-is-better, so the sum sorts ascending. No
        # LIMIT — the outer search() slice paginates; bounding here would re-break
        # total_count / register-scope (see the docstring).
        label_rows = conn.execute(
            "SELECT vc.code_id, vc.code, vc.label, vc.mapping_count, "
            "bm25(value_code_fts) AS rank "
            "FROM value_code_fts "
            "JOIN value_code vc ON vc.code_id = value_code_fts.rowid "
            "WHERE value_code_fts MATCH ?",
            (fts_query,),
        ).fetchall()
        for r in label_rows:
            base = r["rank"] + math.log1p(r["mapping_count"]) * 0.5
            hits[r["code_id"]] = _hit(r, base)

    if _is_code_shaped(query):
        q = query.strip()
        # The owner clause mirrors the build-side `value_code_fts` owner filter in
        # `reg_meta_build/db.py` `_populate_fts` and the owner definition in
        # `_code_owner_annotations_batch` (variables ∪ classifications, no is_valid
        # filter): without it this direct code-shape lookup bypasses the index and
        # leaks context-less hits for exact/prefix code searches (#478). The
        # correlated ref is qualified `value_code.code_id` so it binds to the outer
        # FROM, not classification_code.code_id inside the subquery.
        code_rows = conn.execute(
            "SELECT code_id, code, label, mapping_count "
            "FROM value_code "
            "WHERE (code = ? COLLATE NOCASE OR code LIKE ? ESCAPE '\\') "
            "AND (mapping_count > 0 OR EXISTS ("
            "    SELECT 1 FROM classification_code cc WHERE cc.code_id = value_code.code_id)) "
            "ORDER BY (code = ? COLLATE NOCASE) DESC, length(code), code",
            (q, f"{_escape_like(q)}%", q),
        ).fetchall()
        # Code matches are the strongest signal a code query gives — seed them
        # below the FTS rank floor (a large negative offset) so an exact "F32"
        # outranks any label-text hit; exact before prefix via the -1 nudge. (In
        # the flat `type="all"` path this also puts code-exact hits ahead of other
        # result types for a code-shaped query — the user typed a code; the webapp
        # calls search() per-type, so its groups are unaffected.)
        for i, r in enumerate(code_rows):
            exact = r["code"].casefold() == q.casefold()
            code_rank = -1_000_000 + (0 if exact else 1) + i
            existing = hits.get(r["code_id"])
            if existing is None or code_rank < existing["base_rank"]:
                hits[r["code_id"]] = _hit(r, code_rank)

    ordered = sorted(hits.values(), key=lambda h: (h["base_rank"], h["code"]))

    # Unscoped path (the webapp's `/api/search`, the perf-critical one): a broad
    # term ("läkemedel", "cancer") matches thousands of codes, and annotating ALL
    # of them before the outer offset/limit slice was the >60 s omnibox timeout.
    # Defer owner annotation to the PAGE — return every ranked row unannotated
    # (empty owners) carrying an internal `_code_id` marker, and let `search()`
    # annotate only the ≤limit rows actually shown (`_annotate_value_page`). This
    # keeps total_count = len(ordered) exact (no rows dropped here — there's no
    # register scope to filter against) while doing O(matches) cheap dict-building
    # instead of O(matches) owner queries.
    if reg_ids is None:
        return [
            {
                "type": "code",
                # Glossary rename: SCB `vardekod`/`vardebenamning` surface under the
                # universal English `code`/`label`.
                "code": h["code"],
                "label": h["label"],
                "mapping_count": h["mapping_count"],
                "fts_rank": h["base_rank"],
                "_code_id": h["code_id"],
                **_empty_owner_annotation(),
            }
            for h in ordered
        ]

    # Register-scoped path (CLI `--register`): the match set is small (one
    # register's codes), and the reg-scope DROP below needs every code's owners.
    # Annotate the full set up front + filter — total_count must reflect the
    # post-filter count. These rows carry NO `_code_id` (already annotated).
    owners_by_code = _code_owner_annotations_batch(
        conn, [h["code_id"] for h in ordered], reg_ids
    )

    results: list[dict[str, Any]] = []
    for h in ordered:
        owners = owners_by_code[h["code_id"]]
        # A register scope can leave a code with no surviving owner (no in-scope
        # variable, no classification) — drop it rather than return a context-less
        # code/label pair.
        if not owners["variables"] and not owners["classifications"]:
            continue
        results.append(
            {
                "type": "code",
                # Glossary rename: SCB `vardekod`/`vardebenamning` surface under the
                # universal English `code`/`label`.
                "code": h["code"],
                "label": h["label"],
                "mapping_count": h["mapping_count"],
                "fts_rank": h["base_rank"],
                **owners,
            }
        )
    return results


def _annotate_value_page(
    conn: sqlite3.Connection,
    page: list[dict[str, Any]],
    reg_ids: set[int] | None,
) -> None:
    """Annotate the value/code rows of a SHOWN page with their owning variables /
    classifications, in place (#352 perf, the annotate-only-the-page optimization).

    The unscoped value arm (`_search_values_fts` with `reg_ids is None`) returns
    every ranked code row UNannotated, carrying an internal `_code_id` marker, so
    `search()` can defer the (expensive) owner lookups to the ≤limit rows actually
    paginated. This runs one set-based `_code_owner_annotations_batch` over just
    those page code_ids and merges the result onto each row, then drops the marker.

    No-op when no row carries `_code_id` — i.e. the reg-scoped arm, where rows were
    already annotated up front (the reg-scope drop needed the full owner set). Only
    `type == "code"` rows carry the marker, so a mixed `type="all"` page leaves its
    other-type rows untouched. `reg_ids` is always None whenever marker rows exist
    (they're only produced in the unscoped branch); it is threaded through for
    symmetry with the batch helper's signature."""
    code_ids = [
        r["_code_id"] for r in page if r.get("type") == "code" and "_code_id" in r
    ]
    if not code_ids:
        return
    owners_by_code = _code_owner_annotations_batch(conn, code_ids, reg_ids)
    for r in page:
        if r.get("type") == "code" and "_code_id" in r:
            r.update(owners_by_code[r["_code_id"]])
            del r["_code_id"]


def _classification_leaf(
    slug: str | None,
    short_name: str,
    classification_name: str,
    fts_rank: float,
    classification_id: int,
) -> dict[str, Any]:
    """The classification-leaf result row emitted by both classification search
    arms (name-FTS `_search_classifications` and code-containment
    `_search_classifications_by_code`). One builder so the leaf contract — the
    keys `_fold_concept_groups` (`_classification_id`), pagination, and
    `_strip_internal_keys` depend on — can't drift between the two arms.

    `_classification_id` is the internal fold key (stripped before public): maps
    the leaf to its curated umbrella concept group so the fold can subsume it
    (symmetric with variables' `_variable_id`). A NULL-slug classification isn't
    FQID-addressable (`try_emit` → None), mirroring the catalog enumeration's
    slug filter."""
    return {
        "type": "classification",
        "fqid": try_emit(Fqid.classification_fqid, slug),
        "short_name": short_name,
        "classification_name": classification_name,
        "fts_rank": fts_rank,
        "_classification_id": classification_id,
    }


def _search_classifications(
    conn: sqlite3.Connection, query: str
) -> list[dict[str, Any]]:
    """FTS search over `classification_fts` (#350) — the third shipped FTS index,
    built but previously unsearched (see DESIGN.md → FTS5 configuration). Indexes
    `short_name` + `name` + `name_en` + `description`; `classification_fts.rowid`
    is `classification.id` (content_rowid='id'), so the join recovers the `slug`
    for the `class/<slug>` FQID. A NULL-slug classification isn't FQID-addressable
    (`try_emit` → None), mirroring the catalog enumeration's slug filter."""
    rows = conn.execute(
        "SELECT c.id AS classification_id, cf.short_name, cf.name, c.slug, cf.rank "
        "FROM classification_fts cf "
        "JOIN classification c ON c.id = cf.rowid "
        "WHERE classification_fts MATCH ? "
        "ORDER BY cf.rank",
        (query,),
    ).fetchall()
    return [
        _classification_leaf(
            slug=r["slug"],
            short_name=r["short_name"],
            classification_name=r["name"],
            fts_rank=r["rank"],
            classification_id=r["classification_id"],
        )
        for r in rows
    ]


# Code-containment classification hits rank AFTER all name-FTS hits. Name-FTS
# ranks are bm25 (negative = better), so any positive base sinks below them; the
# enumeration index preserves the SQL order (exact-containing classifications
# first) within the code-containment block.
_CLASS_CODE_RANK_BASE = 1000.0


def _search_classifications_by_code(
    conn: sqlite3.Connection, query: str, exclude_ids: set[int]
) -> list[dict[str, Any]]:
    """Surface the classifications that CONTAIN a code-shaped query (#393 item 5).

    A code-shaped query ("C12", "F32") should find the classification whose code
    SET includes a matching code — "find the classification for this code" — even
    when the query matches no classification NAME (the `_search_classifications`
    FTS arm). The match is exact OR prefix on `value_code.code`. The LIKE matches
    a LITERAL code prefix: the query's LIKE metacharacters (backslash, percent,
    underscore) are escaped via `_escape_like` with an ESCAPE clause, so a
    code-shaped query like "12_" matches literal "12_…" codes, NOT "120"/"129"
    (where a raw underscore would wildcard-match any single char). The exact-match
    parts (`vc.code = ?` and the `has_exact` `=` test) take the RAW `q` — they are
    equality, not LIKE, so they need no escaping.

    The `classification_code` JOIN inherently restricts the result to codes that
    OWN a classification, so the context-less-code drop the value arm needs (#478,
    its `mapping_count > 0 OR EXISTS classification_code` owner filter) is implicit
    here — a code with no `classification_code` row simply doesn't join.

    `exclude_ids` are the classifications already surfaced by the name-FTS arm; we
    skip them so a classification matched by BOTH name and code-containment appears
    ONCE (as its name hit), not twice. Emits rows with the SAME keys as
    `_search_classifications` so they flow identically through `_fold_concept_groups`
    (keyed on `_classification_id`), pagination, and `_strip_internal_keys`.

    `fts_rank` is a POSITIVE base + enumeration index, so these rows sort AFTER all
    (negative-rank) name-FTS hits while preserving the SQL order (exact-containing
    classifications first, then `short_name`). Because `classification.short_name`
    is `NOT NULL UNIQUE` (see reg_meta_build/db.py), `(has_exact DESC, short_name)`
    is already a TOTAL order, so the Python enumeration below freezes a deterministic
    order into `fts_rank` — no extra tiebreak needed.

    The `has_exact` test collates NOCASE to match the WHERE LIKE, which is already
    ASCII-case-insensitive: a lowercase query "c12" admits a stored uppercase "C12"
    via LIKE, so the case-SENSITIVE `=` would score the true exact hit has_exact=0
    and let a prefix-only sibling ("C120") that sorts earlier by short_name rank
    above it. COLLATE NOCASE makes exact-precedence hold for any-case code query
    (the WHERE already surfaces them — LIKE is case-insensitive)."""
    q = query.strip()
    rows = conn.execute(
        "SELECT c.id AS classification_id, c.short_name, c.name AS classification_name, "
        "c.slug, MAX(CASE WHEN vc.code = ? COLLATE NOCASE THEN 1 ELSE 0 END) AS has_exact "
        "FROM value_code vc "
        "JOIN classification_code cc ON cc.code_id = vc.code_id "
        "JOIN classification c ON c.id = cc.classification_id "
        "WHERE vc.code = ? OR vc.code LIKE ? ESCAPE '\\' "
        "GROUP BY c.id, c.short_name, c.name, c.slug "
        "ORDER BY has_exact DESC, c.short_name",
        (q, q, f"{_escape_like(q)}%"),
    ).fetchall()
    kept = [r for r in rows if r["classification_id"] not in exclude_ids]
    return [
        _classification_leaf(
            slug=r["slug"],
            short_name=r["short_name"],
            classification_name=r["classification_name"],
            fts_rank=_CLASS_CODE_RANK_BASE + i,
            classification_id=r["classification_id"],
        )
        for i, r in enumerate(kept)
    ]


# ---------------------------------------------------------------------------
# Concept groups (#322/#325): search folding + the `get groups` listing.
# Read-only over the 5.3.0 group tables; reuses the Catalog read surface
# (`list_concept_groups` / `list_classification_groups`) for member lists so
# the facet-aware member ordering stays in one place.
# ---------------------------------------------------------------------------


def _group_summary_to_dict(summary: ConceptGroupSummary) -> dict[str, Any]:
    """JSON shape for one `ConceptGroupSummary` (see catalog.py): members keep
    their leaf FQIDs — a group is a browse affordance, not an entity."""
    return {
        "key": summary.key,
        "label": summary.label,
        "source": summary.source,
        "axes": list(summary.axes),
        "member_count": len(summary.members),
        "members": [
            {
                "fqid": str(m.fqid),
                "name": m.name,
                "facets": [
                    {"axis": f.axis, "value": f.value, "label": f.label}
                    for f in m.facets
                ],
            }
            for m in summary.members
        ],
    }


def get_concept_groups(conn: sqlite3.Connection, register: str) -> dict[str, Any]:
    """Concept groups for a register (#325), keyed like the other `get`
    commands (name or numeric ID). Reuses `Catalog.list_concept_groups` for
    the group/member/facet shape. A register without a slug isn't catalog-
    addressable and reports an empty group list (the build only derives
    groups over slugged variables, so nothing is hidden)."""
    reg_ids = require_register_ids(conn, register)
    catalog = Catalog(conn)
    registers_out: list[dict[str, Any]] = []
    for rid in reg_ids:
        row = conn.execute(
            "SELECT r.register_id, r.name, r.slug AS register_slug, "
            "p.slug AS provider_slug "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE r.register_id = ?",
            (rid,),
        ).fetchone()
        register_fqid = None
        groups: list[dict[str, Any]] = []
        if row["register_slug"] is not None:
            register_fqid = try_emit(
                Fqid.register_fqid, row["provider_slug"], row["register_slug"]
            )
            groups = [
                _group_summary_to_dict(g)
                for g in catalog.list_concept_groups(
                    row["provider_slug"], row["register_slug"]
                )
            ]
        registers_out.append(
            {
                "register_id": row["register_id"],
                "register_name": row["name"],
                "fqid": register_fqid,
                "groups": groups,
            }
        )
    return {"registers": registers_out}


def get_classification_concept_groups(conn: sqlite3.Connection) -> dict[str, Any]:
    """Curated classification umbrella groups (#325), catalog-scoped. Returns
    only CURATED umbrella entries (see #516 for the first planned content).
    Derived vintage editions (lkf1980…lkf2026, ssyk1996→ssyk2012) surface via
    `classification_replaced_by` succession edges (#571), not here."""
    catalog = Catalog(conn)
    return {
        "groups": [
            _group_summary_to_dict(g) for g in catalog.list_classification_groups()
        ]
    }


def _search_group_labels(
    conn: sqlite3.Connection,
    like_pattern: str,
    reg_ids: set[int] | None,
    *,
    type: str,
    year_range: tuple[int | None, int | None] | None = None,
) -> list[sqlite3.Row]:
    """Concept groups whose LABEL or key matches the query (#322): the family
    itself is findable even when no single leaf row matches (e.g. searching
    "AGI löneinkomst" finds the monthly variable family).

    Scope rules: variable groups respect a `--register` scope and pass the
    'variable' type filter (they fold variable hits); classification groups
    are catalog-scoped, so they only surface unscoped (`type` in
    ("all", "classification"), no register filter) AND with no `year_range`
    (they carry no validity window). `type == "register"` excludes groups
    entirely; `type == "classification"` excludes variable groups. Under a
    `year_range` (--years), a variable group needs at least one member state
    overlapping the range — the group itself has no validity window."""
    if type == "register":
        return []
    rows = conn.execute(
        "SELECT g.group_id, g.kind, g.group_key, g.label, g.source, "
        "g.register_id, r.name AS register_name "
        "FROM concept_group g "
        "LEFT JOIN register r ON r.register_id = g.register_id "
        "WHERE g.label LIKE ? ESCAPE '\\' OR g.group_key LIKE ? ESCAPE '\\' "
        "ORDER BY g.kind, g.group_key",
        (like_pattern, like_pattern),
    ).fetchall()
    hits = []
    for r in rows:
        if r["kind"] == "classification":
            # Classifications are catalog-scoped (no register) and carry no
            # validity window, so a --years filter excludes the family too —
            # symmetric with the classification-leaf exclusion in
            # `_filter_search_by_years` (#350).
            if reg_ids or type == "variable" or year_range is not None:
                continue
        else:
            # A variable-kind group has no place in a classifications-only query.
            if type == "classification":
                continue
            if reg_ids and r["register_id"] not in reg_ids:
                continue
            if year_range is not None and not _group_member_state_in_years(
                conn, r["group_id"], *year_range
            ):
                continue
        hits.append(r)
    return hits


def _group_member_state_in_years(
    conn: sqlite3.Connection, group_id: int, lo: int | None, hi: int | None
) -> bool:
    """Whether any member variable of a variable-kind group has a
    `variable_state` validity window overlapping the requested year range —
    the --years semantics for a label-matched group (mirrors what
    `_filter_search_by_years` does for leaf hits)."""
    rows = conn.execute(
        "SELECT vs.valid_from, vs.valid_to "
        "FROM concept_group_variable cgv "
        "JOIN variable_state vs ON vs.variable_id = cgv.variable_id "
        "WHERE cgv.group_id = ?",
        (group_id,),
    ).fetchall()
    return any(
        _state_overlaps_years(r["valid_from"], r["valid_to"], lo, hi) for r in rows
    )


def _terminal_classification_slug(conn: sqlite3.Connection, slug: str) -> str:
    """Walk `classification_replaced_by` (#571) from a classification edition slug
    to its TERMINAL (current) successor — the chain end with no outbound edge —
    and return that slug. Returns the input unchanged when it's already terminal.

    STOP AT A SPLIT (#604): a 1→many split predecessor (e.g. `sun1996` →
    {`sun-niva2000`, `sun-inriktning2000`, `sun-grupp2000`}) has NO single terminal
    — the chain branches into several distinct current editions. The walk stops at
    such a node and returns IT as its own terminal, rather than arbitrarily
    following the lexicographically-first branch (which would mislabel a `sun1996`
    hit with a single branch's terminal). A node with exactly ONE successor is a
    plain rename — walk to it (a linear chain still collapses to its real terminal).
    `seen` cycle guard mirrors `Catalog._walk_terminal`."""
    seen = {slug}
    current = slug
    while True:
        rows = conn.execute(
            "SELECT successor_slug FROM classification_replaced_by "
            "WHERE predecessor_slug = ?",
            (current,),
        ).fetchall()
        # No outbound edge → terminal. >1 outbound edge → a split; the node is its
        # own terminal (the chain branches, no single current edition).
        if len(rows) != 1:
            break
        successor = rows[0]["successor_slug"]
        if successor in seen:
            break  # defensive cycle guard
        current = successor
        seen.add(current)
    return current


def _fold_classification_succession(
    conn: sqlite3.Connection, results: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Collapse classification EDITION hits sharing a succession chain (#571) into
    one row for the chain's TERMINAL (current) edition, carrying the non-terminal
    editions as history.

    Succession editions are NOT `concept_group_classification` members (#571 moved
    them to `classification_replaced_by`), so they need a SEPARATE fold from
    `_fold_concept_groups`. This runs BEFORE it: once editions collapse to their
    terminal, the curated SUN-style umbrella group (#516) folds the terminal
    editions into `group:sun` cleanly (the emitted row keeps the terminal's
    `_classification_id`, so the umbrella pass treats it as that classification).

    A chain with ≥2 DISTINCT editions present in the results collapses to a single
    `type: "classification_succession"` row. A LONE edition hit (whether terminal
    or an old vintage) is NOT a chain-in-the-results signal, so it stays a leaf —
    mirroring `_fold_concept_groups`' lone-member convention; an old-vintage leaf
    is annotated with its `terminal_fqid` so the webapp can still link "current".
    Result-shaping only; folds before pagination, so a chain counts as one
    result."""
    # Map each classification leaf's id → its slug (the succession table keys on
    # slug). Leaves carry only `_classification_id`; recover the slug in one query.
    cids = {
        r["_classification_id"]
        for r in results
        if r.get("type") == "classification" and r.get("_classification_id") is not None
    }
    if not cids:
        return results
    placeholders = ",".join("?" * len(cids))
    slug_by_id = {
        row["id"]: row["slug"]
        for row in conn.execute(
            f"SELECT id, slug FROM classification WHERE id IN ({placeholders})",
            list(cids),
        ).fetchall()
        if row["slug"] is not None
    }

    # Group classification leaf hits by their chain's terminal slug.
    buckets: dict[str, list[dict[str, Any]]] = {}
    terminal_by_hit: dict[int, str] = {}  # id(row) → terminal slug
    for r in results:
        if r.get("type") != "classification":
            continue
        cid = r.get("_classification_id")
        slug = slug_by_id.get(cid) if cid is not None else None
        if slug is None:
            continue  # NULL-slug classification isn't a succession participant
        terminal = _terminal_classification_slug(conn, slug)
        buckets.setdefault(terminal, []).append(r)
        terminal_by_hit[id(r)] = terminal

    # A chain collapses only when ≥2 DISTINCT editions are present — a single
    # edition hit (terminal or old vintage) isn't a chain-in-results signal and
    # stays a leaf (mirrors `_fold_concept_groups`' lone-member rule).
    collapse: set[str] = set()
    for terminal, hits in buckets.items():
        distinct = {slug_by_id[h["_classification_id"]] for h in hits}
        if len(distinct) >= 2:
            collapse.add(terminal)

    out: list[dict[str, Any]] = []
    emitted: set[str] = set()
    for r in results:
        if r.get("type") != "classification":
            out.append(r)
            continue
        terminal = terminal_by_hit.get(id(r))
        if terminal is None or terminal not in collapse:
            # Lone edition (or a NULL-slug leaf): stays a leaf, annotated with its
            # terminal so the webapp can link "current" from an old vintage hit.
            # A split root (#604) or an already-terminal edition resolves to ITSELF
            # (`_terminal_classification_slug` stops at a split), so `terminal ==`
            # its own slug → the guard below skips the annotation: no misleading
            # single-branch "current" on a node whose chain actually branches.
            if terminal is not None and terminal != slug_by_id.get(
                r.get("_classification_id")
            ):
                r["terminal_fqid"] = try_emit(Fqid.classification_fqid, terminal)
            out.append(r)
        elif terminal not in emitted:
            emitted.add(terminal)
            out.append(
                _classification_succession_row(conn, terminal, buckets[terminal])
            )
    return out


def _classification_succession_row(
    conn: sqlite3.Connection, terminal_slug: str, matched: list[dict[str, Any]]
) -> dict[str, Any]:
    """One `type: "classification_succession"` row for a collapsed edition chain
    (#571): the terminal (current) edition's identity + the full edition list
    (terminal-first by BFS depth — see `_classification_editions`) + the leaf hits
    it folded (`matched`). Keeps the terminal's `_classification_id` so the
    downstream umbrella fold (`_fold_concept_groups`, #516) can subsume it into
    `group:sun`.
    `fts_rank` is the best (lowest) matched-hit rank so it sorts where its hits
    would have."""
    terminal = conn.execute(
        "SELECT id, slug, short_name, name FROM classification WHERE slug = ?",
        (terminal_slug,),
    ).fetchone()
    editions = _classification_editions(conn, terminal_slug)
    # `_classification_id` lets the umbrella pass treat this as the terminal
    # classification; None when the terminal itself has no live row (a dead chain
    # end), which simply means no umbrella fold can pick it up.
    return {
        "type": "classification_succession",
        "fqid": try_emit(Fqid.classification_fqid, terminal_slug),
        "short_name": terminal["short_name"] if terminal else None,
        "classification_name": terminal["name"] if terminal else None,
        "editions": editions,
        "matched": matched,
        "fts_rank": min((h.get("fts_rank", 0) for h in matched), default=0),
        "_classification_id": terminal["id"] if terminal else None,
    }


def _classification_editions(
    conn: sqlite3.Connection, terminal_slug: str
) -> list[dict[str, Any]]:
    """The full edition list of a succession chain ending at `terminal_slug`,
    ordered terminal-first by BFS DEPTH from the terminal (#571/#588). Walks the
    predecessor side of `classification_replaced_by` transitively from the terminal,
    hydrating each edition's `name`/`effective_year` (None when the edition has no
    live `classification` row — a dead predecessor).

    This is the TERMINAL-CENTRIC fold (no queried node — it collapses a whole family
    onto its terminal), so collect-all-ancestors is correct here, unlike
    `Catalog.classification_chain` which anchors on the queried path. Only the
    ORDERING differs: ordering by BFS depth (terminal = depth 0, its predecessors
    depth 1, …) is robust to undated/NULL `effective_year` edges (the old
    descending-`effective_year` sort inverted on them), and is behavior-preserving
    for the linear+dated corpus (depth-ascending == terminal-first-newest-to-oldest).
    `effective_year` stays as a display field on each edition, no longer the sort
    key."""
    # The terminal carries no `effective_year` of its own (it's the head, not a
    # successor in any edge); predecessors carry the year on the edge that names
    # them as `successor_slug`. `depth_by_slug` records each slug's BFS level from
    # the terminal — the order key (the walk order, not the date).
    year_by_slug: dict[str, int | None] = {terminal_slug: None}
    depth_by_slug: dict[str, int] = {terminal_slug: 0}
    seen = {terminal_slug}
    frontier = [terminal_slug]
    depth = 0
    while frontier:
        depth += 1
        nxt: list[str] = []
        placeholders = ",".join("?" * len(frontier))
        rows = conn.execute(
            "SELECT predecessor_slug, effective_year FROM classification_replaced_by "
            f"WHERE successor_slug IN ({placeholders})",
            frontier,
        ).fetchall()
        for row in rows:
            pred = row["predecessor_slug"]
            if pred in seen:
                continue
            seen.add(pred)
            year_by_slug[pred] = row["effective_year"]
            depth_by_slug[pred] = depth
            nxt.append(pred)
        frontier = nxt

    name_by_slug = {
        row["slug"]: row["name"]
        for row in conn.execute(
            "SELECT slug, name FROM classification WHERE slug IN ("
            + ",".join("?" * len(year_by_slug))
            + ")",
            list(year_by_slug),
        ).fetchall()
    }
    editions = [
        {
            "slug": slug,
            "fqid": try_emit(Fqid.classification_fqid, slug),
            "name": name_by_slug.get(slug),
            "effective_year": year,
        }
        for slug, year in year_by_slug.items()
    ]
    # Terminal-first by BFS depth (depth 0 = terminal); slug a stable tiebreak among
    # same-depth predecessors. Date-independent → robust to undated edges.
    editions.sort(key=lambda e: (depth_by_slug[e["slug"]], e["slug"]))
    return editions


def _fold_concept_groups(
    conn: sqlite3.Connection,
    results: list[dict[str, Any]],
    label_hits: list[sqlite3.Row],
) -> list[dict[str, Any]]:
    """Collapse sibling search hits under their concept group (#322).

    A group row replaces its member hits when ≥2 DISTINCT members matched (one
    member hit through several fields is not a family signal) OR the group's own
    label matched. A lone member hit stays a leaf row, annotated with
    `concept_group`/`concept_group_label` so the family is still discoverable.
    Original leaf hits ride under the group row's `matched`; the full member list
    (facet-ordered, via the Catalog reuse) rides under `members`.

    Both member kinds fold symmetrically: variable leaves key on `_variable_id`
    (mapped via `concept_group_variable`), classification leaves on
    `_classification_id` (via `concept_group_classification`). The
    classification arm handles curated umbrella groups (#516); derived vintage
    edition families are no longer in `concept_group_classification` (#571 moved
    them to `classification_replaced_by` succession edges), so it produces no
    hits until curated umbrella content ships."""
    membership = _member_group_index(conn, results)

    buckets: dict[int, list[dict[str, Any]]] = {}
    group_meta: dict[int, sqlite3.Row] = {}
    for r in results:
        key = _member_key(r)
        member = membership.get(key) if key is not None else None
        if member is not None:
            buckets.setdefault(member["group_id"], []).append(r)
            group_meta.setdefault(member["group_id"], member)

    label_ids = {row["group_id"] for row in label_hits}
    for row in label_hits:
        group_meta.setdefault(row["group_id"], row)

    folded_ids = set(label_ids)
    for gid, hits in buckets.items():
        if len({_member_key(h) for h in hits}) >= 2:
            folded_ids.add(gid)

    summaries = _GroupSummaryLookup(conn)
    out: list[dict[str, Any]] = []
    emitted: set[int] = set()
    for r in results:
        key = _member_key(r)
        member = membership.get(key) if key is not None else None
        if member is None:
            out.append(r)
            continue
        gid = member["group_id"]
        if gid not in folded_ids:
            r["concept_group"] = member["group_key"]
            r["concept_group_label"] = member["label"]
            out.append(r)
        elif gid not in emitted:
            emitted.add(gid)
            out.append(
                _group_result_row(
                    group_meta[gid],
                    buckets[gid],
                    summaries,
                    label_matched=gid in label_ids,
                )
            )
    for row in label_hits:
        if row["group_id"] not in emitted:
            emitted.add(row["group_id"])
            out.append(_group_result_row(row, [], summaries, label_matched=True))
    return out


def _member_key(r: dict[str, Any]) -> tuple[str, int] | None:
    """The (kind, id) concept-group membership key for a leaf result row, or
    None for a row that can't be a member (e.g. a register hit). Kind-tagged so
    a variable_id and a classification_id can't collide across the two tables."""
    vid = r.get("_variable_id")
    if vid is not None:
        return ("variable", vid)
    cid = r.get("_classification_id")
    if cid is not None:
        return ("classification", cid)
    return None


def _member_group_index(
    conn: sqlite3.Connection, results: list[dict[str, Any]]
) -> dict[tuple[str, int], sqlite3.Row]:
    """Map each member leaf's (kind, id) key to its `concept_group` row, across
    BOTH membership tables (`concept_group_variable` / `concept_group_classification`)."""
    index: dict[tuple[str, int], sqlite3.Row] = {}
    vids = {r["_variable_id"] for r in results if r.get("_variable_id") is not None}
    if vids:
        placeholders = ",".join("?" * len(vids))
        rows = conn.execute(
            "SELECT cgv.variable_id AS member_id, g.group_id, g.kind, g.group_key, "
            "g.label, g.source, g.register_id, r.name AS register_name "
            "FROM concept_group_variable cgv "
            "JOIN concept_group g ON g.group_id = cgv.group_id "
            "LEFT JOIN register r ON r.register_id = g.register_id "
            f"WHERE cgv.variable_id IN ({placeholders})",
            list(vids),
        ).fetchall()
        for row in rows:
            index[("variable", row["member_id"])] = row
    cids = {
        r["_classification_id"]
        for r in results
        if r.get("_classification_id") is not None
    }
    if cids:
        placeholders = ",".join("?" * len(cids))
        rows = conn.execute(
            "SELECT cgc.classification_id AS member_id, g.group_id, g.kind, "
            "g.group_key, g.label, g.source, g.register_id, r.name AS register_name "
            "FROM concept_group_classification cgc "
            "JOIN concept_group g ON g.group_id = cgc.group_id "
            "LEFT JOIN register r ON r.register_id = g.register_id "
            f"WHERE cgc.classification_id IN ({placeholders})",
            list(cids),
        ).fetchall()
        for row in rows:
            index[("classification", row["member_id"])] = row
    return index


class _GroupSummaryLookup:
    """Per-search cache over the Catalog group listings, so folding N groups
    of one register costs one `list_concept_groups` call, not N."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._catalog = Catalog(conn)
        self._by_scope: dict[int | None, dict[str, ConceptGroupSummary]] = {}

    def get(self, meta: sqlite3.Row) -> ConceptGroupSummary | None:
        """The `ConceptGroupSummary` for a `concept_group` row, or None when
        the register isn't slugged (no catalog address → no member listing;
        can't happen for build-derived groups, which only cover slugged
        variables)."""
        scope = None if meta["kind"] == "classification" else meta["register_id"]
        if scope not in self._by_scope:
            self._by_scope[scope] = self._load_scope(scope)
        return self._by_scope[scope].get(meta["group_key"])

    def _load_scope(self, scope: int | None) -> dict[str, ConceptGroupSummary]:
        if scope is None:
            groups = self._catalog.list_classification_groups()
        else:
            row = self._conn.execute(
                "SELECT r.slug AS register_slug, p.slug AS provider_slug "
                "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
                "WHERE r.register_id = ?",
                (scope,),
            ).fetchone()
            if row is None or row["register_slug"] is None:
                return {}
            groups = self._catalog.list_concept_groups(
                row["provider_slug"], row["register_slug"]
            )
        return {g.key: g for g in groups}


def _group_result_row(
    meta: sqlite3.Row,
    matched: list[dict[str, Any]],
    summaries: _GroupSummaryLookup,
    *,
    label_matched: bool,
) -> dict[str, Any]:
    """One `type: "group"` search result row: group identity + the full
    member list + the leaf hits it folded (`matched`). `fts_rank` is the best
    (lowest) member rank so the group sorts where its members would have."""
    summary = summaries.get(meta)
    payload = (
        _group_summary_to_dict(summary)
        if summary is not None
        else {"axes": [], "member_count": 0, "members": []}
    )
    return {
        "type": "group",
        "kind": meta["kind"],
        "group_key": meta["group_key"],
        "group_label": meta["label"],
        "group_source": meta["source"],
        "register_id": meta["register_id"],
        "register_name": meta["register_name"],
        "axes": payload["axes"],
        "member_count": payload["member_count"],
        "members": payload["members"],
        "matched": matched,
        "label_matched": label_matched,
        "fts_rank": min((h.get("fts_rank", 0) for h in matched), default=0),
    }


# `_code_id` is the value-arm's deferred-annotation marker (#352 perf):
# `_annotate_value_page` removes it on the shown page, but list it here so a stray
# marker can never leak past `_strip_internal_keys` into a public result row.
_INTERNAL_KEYS = ("_variable_id", "_classification_id", "_code_id")


def _strip_internal_keys(results: list[dict[str, Any]]) -> None:
    """Drop the fold-internal member keys (`_variable_id` / `_classification_id`)
    from leaf rows and from the hits nested under any `matched` before results go
    public. Recurses, because `matched` can nest two deep: the succession fold
    (#571) emits a `classification_succession` row carrying its own `matched`,
    which the umbrella fold (#516) can then re-nest under a group row's `matched`."""
    for r in results:
        for key in _INTERNAL_KEYS:
            r.pop(key, None)
        matched = r.get("matched")
        if matched:
            _strip_internal_keys(matched)


# ---------------------------------------------------------------------------
# Get register
# ---------------------------------------------------------------------------


def get_register(
    conn: sqlite3.Connection,
    register: str,
) -> list[dict[str, Any]]:
    """Get register(s) by name or ID with variants.

    Returns a list of register dicts, each with a "variants" key.
    """
    reg_ids = require_register_ids(conn, register)

    registers = []
    for rid in reg_ids:
        # SELECT * picks up the renamed `name` and `purpose` columns; the
        # row dict surfaces them under those keys (consumers updated).
        reg = conn.execute(
            "SELECT r.*, p.slug AS provider_slug "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE r.register_id = ?",
            (rid,),
        ).fetchone()
        entry = dict(reg)
        provider_slug = entry.pop("provider_slug")
        register_fqid = try_emit(Fqid.register_fqid, provider_slug, entry["slug"])
        entry["fqid"] = register_fqid
        variants = conn.execute(
            "SELECT * FROM register_variant WHERE register_id = ? ORDER BY register_variant_id",
            (rid,),
        ).fetchall()
        variant_dicts: list[dict[str, Any]] = []
        for v in variants:
            vd = dict(v)
            # A2.6: a variant is a navigational sub-resource of a register, not a
            # slash-path FQID. It carries the parent
            # register FQID + its own slug (the `?variant=` browse coordinate),
            # not an addressable variant FQID.
            vd["register_fqid"] = register_fqid
            variant_dicts.append(vd)
        entry["variants"] = variant_dicts
        registers.append(entry)
    return registers


# ---------------------------------------------------------------------------
# Get schema
# ---------------------------------------------------------------------------


def _in_placeholders(ids: Iterable[object]) -> str:
    return ",".join("?" for _ in ids)


def get_schema(
    conn: sqlite3.Connection,
    *,
    register_variant_id: str | None = None,
    register: str | None = None,
    years: str | None = None,
    columns_like: str | None = None,
) -> dict[str, Any]:
    """Get column listing organized by variant → version → columns.

    Requires either register_variant_id or register. Returns {"variants": [...]}.
    """
    variant_select = (
        "SELECT rv.*, p.slug AS provider_slug, r.slug AS register_slug "
        "FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
    )
    if register_variant_id:
        rv = conn.execute(
            variant_select + "WHERE rv.register_variant_id = ?",
            (_try_int(register_variant_id),),
        ).fetchone()
        if not rv:
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"Register variant {register_variant_id} not found.",
                remediation="Use `reg-meta get register <name>` to list variants.",
            )
        variant_rows = [rv]
    elif register:
        reg_ids = require_register_ids(conn, register)
        variant_rows = conn.execute(
            variant_select + f"WHERE rv.register_id IN ({_in_placeholders(reg_ids)}) "
            "ORDER BY rv.register_id, rv.register_variant_id",
            reg_ids,
        ).fetchall()
        if not variant_rows:
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"No variants found for register '{register}'.",
                remediation="Use `reg-meta get register <name>` to verify.",
            )
    else:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="Provide either a register_variant_id or register.",
            remediation="Usage: get_schema(conn, register_variant_id=...) or get_schema(conn, register=...)",
        )

    year_lo, year_hi = None, None
    if years:
        year_lo, year_hi = parse_year_range(years)

    variants_out: list[dict[str, Any]] = []
    for rv in variant_rows:
        rvid = rv["register_variant_id"]
        provider_slug = rv["provider_slug"]
        register_slug = rv["register_slug"]
        # A2.6: schema is organized by `variable_state` editions now (validity
        # windows), not the dropped `register_version`. One "version" per
        # distinct (valid_from, valid_to) DELIVERY WINDOW the variant delivered;
        # its columns are every state in that window. `value_set_version_label`
        # is a PER-COLUMN attribute (a folded multi-vintage variable — see reg_meta_build/DESIGN.md → Build-time triage (SCB) — carries
        # two states in the SAME window with labels like `sni92`/`sni2007` while
        # ordinary columns carry ''), so it must NOT be part of the edition key —
        # keying by it would shard one delivered schema into partial pseudo-
        # versions. The binding FQID is 3-seg (provider/register/variable_slug).
        # #325: LEFT JOIN the concept-group membership (at most one group per
        # variable — single-column member PK) so the fold is visible inline on
        # schema listings. NULL `concept_group` means "not in any group".
        state_rows = conn.execute(
            "SELECT vs.valid_from, vs.valid_to, vs.value_set_version_label, "
            "vs.data_type, vs.data_length, vs.delivery_column_name, "
            "v.variable_id, " + _VAR_ID_V + ", v.slug AS variable_slug, "
            "v.name AS variable_name, COALESCE(v.source_label, '') AS source, "
            "cg.group_key AS concept_group, cg.label AS concept_group_label "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "LEFT JOIN concept_group_variable cgv ON cgv.variable_id = v.variable_id "
            "LEFT JOIN concept_group cg ON cg.group_id = cgv.group_id "
            "WHERE vs.register_variant_id = ? "
            "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, v.slug",
            (rvid,),
        ).fetchall()

        # Group states into editions keyed by the DELIVERY WINDOW only,
        # preserving first-seen order for determinism. One edition per window
        # holds ALL columns delivered then — including every vintage-state of a
        # folded variable; the label rides on each column below.
        editions: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for s in state_rows:
            editions.setdefault((s["valid_from"], s["valid_to"]), []).append(s)

        versions_out: list[dict[str, Any]] = []
        for (valid_from, valid_to), states in editions.items():
            # A2.6: filter by validity-window OVERLAP against the requested
            # years, not the opening year alone — a multi-year or open-ended
            # edition must survive a filter for any year it spans. `year` below
            # stays the opening year for display.
            if years and not _state_overlaps_years(
                valid_from, valid_to, year_lo, year_hi
            ):
                continue
            year = int(valid_from[:4])

            col_dicts: list[dict[str, Any]] = []
            for s in states:
                col_dicts.append(
                    {
                        # `variable_id` (always present + unique) is the key
                        # `compare()` dedups on (#474); `var_id` rides along as
                        # the display value (blank for non-SCB).
                        "variable_id": s["variable_id"],
                        "var_id": s["var_id"],
                        "data_type": s["data_type"],
                        "data_length": s["data_length"],
                        "variable_name": s["variable_name"],
                        "source": s["source"],
                        # Per-column vintage discriminator (see reg_meta_build/DESIGN.md → Build-time triage (SCB)): '' for ordinary
                        # columns, e.g. `sni92`/`sni2007` for the two states of a
                        # folded multi-vintage variable sharing this window.
                        "value_set_version_label": s["value_set_version_label"],
                        # The state's denormalized latest alias (see DESIGN.md → Two-level variable model) is the
                        # display column; emit it under `aliases` for the
                        # table/flat renderers and `compare()` flattening.
                        "aliases": s["delivery_column_name"] or "",
                        # #325: concept-group membership (None when ungrouped)
                        # so the fold is visible inline; `get groups` lists the
                        # full family with facets.
                        "concept_group": s["concept_group"],
                        "concept_group_label": s["concept_group_label"],
                        "fqid": try_emit(
                            Fqid.binding_fqid,
                            provider_slug,
                            register_slug,
                            s["variable_slug"],
                        ),
                    }
                )
            if columns_like:
                pattern = re.compile(columns_like, re.IGNORECASE)
                col_dicts = [
                    c
                    for c in col_dicts
                    if pattern.search(c.get("aliases") or "")
                    or pattern.search(c.get("variable_name") or "")
                ]

            versions_out.append(
                {
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                    "year": year,
                    "columns": col_dicts,
                }
            )

        if versions_out:
            variants_out.append(
                {
                    "register_variant_id": rvid,
                    "register_id": rv["register_id"],
                    # Glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): the variant's name + description (was Swedish
                    # `registervariantnamn` / `registervariantbeskrivning`).
                    "variant_name": rv["name"],
                    "variant_description": rv["description"],
                    # A2.6: a variant has no slash-path FQID; it carries the
                    # parent register FQID + its browse slug.
                    "register_fqid": try_emit(
                        Fqid.register_fqid, provider_slug, register_slug
                    ),
                    "variant": rv["slug"],
                    "versions": versions_out,
                }
            )

    return {"variants": variants_out}


# ---------------------------------------------------------------------------
# Get varinfo
# ---------------------------------------------------------------------------


def get_varinfo(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
) -> list[dict[str, Any]]:
    """Get detailed variable information.

    Returns a list of variable dicts, each with an "instances" key.
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    # Match variable by var_id first, fall back to name
    int_variable = _try_int(variable)
    # SELECT v.* surfaces the renamed `name`/`definition`/`description`
    # columns directly. `r.name AS register_name` disambiguates the join.
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        vars_by_id = conn.execute(
            "SELECT v.*, " + _VAR_ID_V + ", r.name AS register_name FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE v.provider_key = CAST(? AS TEXT) AND v.register_id IN ({ph})",
            [int_variable, *reg_ids],
        ).fetchall()
        vars_by_name = conn.execute(
            "SELECT v.*, " + _VAR_ID_V + ", r.name AS register_name FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE LOWER(v.name) = LOWER(?) AND v.register_id IN ({ph})",
            [variable, *reg_ids],
        ).fetchall()
    else:
        vars_by_id = conn.execute(
            "SELECT v.*, " + _VAR_ID_V + ", r.name AS register_name FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE v.provider_key = CAST(? AS TEXT)",
            (int_variable,),
        ).fetchall()
        vars_by_name = conn.execute(
            "SELECT v.*, " + _VAR_ID_V + ", r.name AS register_name FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE LOWER(v.name) = LOWER(?)",
            (variable,),
        ).fetchall()

    matched_vars = vars_by_id or vars_by_name

    # Fall back to alias (column name) lookup
    if not matched_vars:
        # A2.7: `variable_alias` is variable_id-keyed; join straight to `variable`.
        alias_sql = (
            "SELECT DISTINCT v.*, "
            + _VAR_ID_V
            + ", r.name AS register_name FROM variable_alias a "
            "JOIN variable v ON a.variable_id = v.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE LOWER(a.delivery_column_name) = LOWER(?)"
        )
        if reg_ids:
            ph = _in_placeholders(reg_ids)
            alias_sql += f" AND v.register_id IN ({ph})"
            matched_vars = conn.execute(alias_sql, [variable, *reg_ids]).fetchall()
        else:
            matched_vars = conn.execute(alias_sql, (variable,)).fetchall()

    if not matched_vars:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `reg-meta search --query <term>` to find variables.",
        )

    variables_out: list[dict[str, Any]] = []
    for var in matched_vars:
        rid, vid = var["register_id"], var["var_id"]
        variable_id = var["variable_id"]

        # A2.6: "instances" are `variable_state` rows now (per-delivery shape),
        # not per-cvid `variable_instance` × `register_version` rows. Each state
        # carries its variant + validity window + value-set version; the year
        # comes from `valid_from`. The 3-seg binding FQID is built from the
        # state's own variable slug — split siblings each surface their own slug,
        # not a shared `(register_id, var_id)` pick.
        #
        # Select states by the matched row's `variable_id`, NOT by
        # `(register_id, provider_key)`: `provider_key` is NON-unique after an
        # A2.2 split (siblings share one source key), so a provider_key filter
        # would fan in every sibling's states under this one matched variable.
        states = conn.execute(
            "SELECT vs.state_id, vs.register_variant_id, vs.valid_from, vs.valid_to, "
            "vs.value_set_version_label, vs.data_type, vs.data_length, "
            "vs.delivery_column_name, vs.value_set_id, "
            "rv.name AS variant_name, "
            "p.slug AS provider_slug, r.slug AS register_slug, v.slug AS variable_slug "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "JOIN register_variant rv ON vs.register_variant_id = rv.register_variant_id "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE vs.variable_id = ? "
            "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, "
            "vs.register_variant_id, vs.state_id",
            (variable_id,),
        ).fetchall()

        # Value-set member counts per value_set_id (None when the state has no
        # codes). Batched so a wide variable doesn't fan out N+1 queries.
        vs_ids = {s["value_set_id"] for s in states if s["value_set_id"] is not None}
        value_counts: dict[int, int] = dict.fromkeys(vs_ids, 0)
        if vs_ids:
            vs_ph = _in_placeholders(vs_ids)
            for row in conn.execute(
                f"SELECT value_set_id, COUNT(*) AS cnt FROM value_set_member "
                f"WHERE value_set_id IN ({vs_ph}) GROUP BY value_set_id",
                list(vs_ids),
            ):
                value_counts[row["value_set_id"]] = row["cnt"]

        instances_out: list[dict[str, Any]] = []
        for s in states:
            col = s["delivery_column_name"]
            inst_dict: dict[str, Any] = {
                "state_id": s["state_id"],
                "register_variant_id": s["register_variant_id"],
                "variant_name": s["variant_name"],
                "valid_from": s["valid_from"],
                "valid_to": s["valid_to"],
                "value_set_version_label": s["value_set_version_label"],
                "year": int(s["valid_from"][:4]),
                "data_type": s["data_type"],
                "data_length": s["data_length"],
                # The state's denormalized latest alias (see DESIGN.md → Two-level variable model); list-shaped for
                # the CLI renderer's `", ".join(...)`.
                "aliases": [col] if col else [],
                "value_set_count": (
                    value_counts.get(s["value_set_id"], 0)
                    if s["value_set_id"] is not None
                    else 0
                ),
                "fqid": try_emit(
                    Fqid.binding_fqid,
                    s["provider_slug"],
                    s["register_slug"],
                    s["variable_slug"],
                ),
            }
            instances_out.append(inst_dict)

        var_classifications = classifications_for_variable(conn, variable_id)

        variables_out.append(
            {
                "register_id": rid,
                "register_name": var["register_name"],
                "var_id": vid,
                # Glossary-rename keys (see DESIGN.md → Glossary and Swedish↔English crosswalk): SCB Swedish columns surface here as the
                # universal English names. Dropped columns
                # (variabelreferenstid, variabelhamtadfran,
                # variabelextern_kommentar, variabeloperationell_definition)
                # no longer appear; their values (where meaningful) were
                # folded into `description` at ingest.
                "name": var["name"],
                "definition": var["definition"],
                "description": var["description"],
                "source_register_text": var["source_register_text"],
                "measurement_unit": var["measurement_unit"],
                "classifications": var_classifications,
                "instances": instances_out,
            }
        )

    return variables_out


# ---------------------------------------------------------------------------
# Get availability
# ---------------------------------------------------------------------------


def get_availability(
    conn: sqlite3.Connection,
    target: str,
    *,
    register: str | None = None,
) -> dict[str, Any]:
    """Return temporal availability summary for a variable or register.

    Auto-detects target type: tries variable first, falls back to register.
    """
    result = _get_availability_variable(conn, target, register=register)
    if result is not None:
        return result

    result = _get_availability_register(conn, target)
    if result is not None:
        return result

    raise RegMetaError(
        exit_code=EXIT_NOT_FOUND,
        code="not_found",
        error_class="query",
        message=f"No variable or register matching '{target}'.",
        remediation="Use `reg-meta search` to find valid names or IDs.",
    )


def _get_availability_variable(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
) -> dict[str, Any] | None:
    """Availability for a variable across registers and years."""
    int_variable = _try_int(variable)

    reg_filter = ""
    params: list = [int_variable, variable]
    if register:
        ids = resolve_register_ids(conn, register)
        if not ids:
            return None
        ph = _in_placeholders(ids)
        reg_filter = f" AND v.register_id IN ({ph})"
        params.extend(ids)

    var_rows = conn.execute(
        "SELECT v.variable_id, v.register_id, "
        + _VAR_ID_V
        + ", v.name AS variable_name, "
        "r.name AS register_name FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        f"WHERE (v.provider_key = CAST(? AS TEXT) OR LOWER(v.name) = LOWER(?)){reg_filter}",
        params,
    ).fetchall()

    if not var_rows:
        return None

    # Gather all version years and aliases per (register, year)
    all_years: set[int] = set()
    registers_out: list[dict[str, Any]] = []

    for var in var_rows:
        rid = var["register_id"]
        vid = var["var_id"]
        variable_id = var["variable_id"]

        # A2.6: year coverage comes from `variable_state` validity windows
        # (register_version is dropped before ship). Each state contributes the
        # calendar years its window spans; its delivery column is the per-year
        # alias.
        #
        # Select by the matched `variable_id`, NOT `(register_id, provider_key)`:
        # `provider_key` is NON-unique after an A2.2 split, so a provider_key
        # filter would credit one sibling with every sibling's year coverage.
        rows = conn.execute(
            "SELECT vs.valid_from, vs.valid_to, vs.delivery_column_name "
            "FROM variable_state vs "
            "WHERE vs.variable_id = ? "
            "ORDER BY vs.valid_from, vs.valid_to",
            (variable_id,),
        ).fetchall()

        reg_years: list[int] = []
        aliases_by_year: dict[str, list[str]] = {}
        for row in rows:
            col = row["delivery_column_name"]
            for year in _years_in_range(row["valid_from"], row["valid_to"]):
                reg_years.append(year)
                all_years.add(year)
                bucket = aliases_by_year.setdefault(str(year), [])
                if col and col not in bucket:
                    bucket.append(col)

        if not reg_years:
            continue

        reg_years_sorted = sorted(set(reg_years))
        min_y, max_y = reg_years_sorted[0], reg_years_sorted[-1]
        expected = set(range(min_y, max_y + 1))
        gaps = sorted(expected - set(reg_years_sorted))

        registers_out.append(
            {
                "register_id": rid,
                "register_name": var["register_name"],
                "var_id": vid,
                "min_year": min_y,
                "max_year": max_y,
                "years": reg_years_sorted,
                "gaps": gaps,
                "aliases_by_year": aliases_by_year,
            }
        )

    if not registers_out:
        return None

    all_years_sorted = sorted(all_years)
    min_y = all_years_sorted[0]
    max_y = all_years_sorted[-1]
    expected = set(range(min_y, max_y + 1))
    gaps = sorted(expected - all_years)

    return {
        "target": variable,
        "target_type": "variable",
        "variable_name": var_rows[0]["variable_name"],
        "min_year": min_y,
        "max_year": max_y,
        "years": all_years_sorted,
        "gaps": gaps,
        "register_count": len(registers_out),
        "registers": registers_out,
    }


def _get_availability_register(
    conn: sqlite3.Connection,
    register: str,
) -> dict[str, Any] | None:
    """Availability for a register across years."""
    ids = resolve_register_ids(conn, register)
    if not ids:
        return None

    # Use first match
    reg_id = ids[0]
    reg = conn.execute(
        "SELECT name FROM register WHERE register_id = ?", (reg_id,)
    ).fetchone()

    # A2.6: year coverage per variant comes from `variable_state` validity
    # windows (register_version is dropped before ship).
    rows = conn.execute(
        "SELECT rvar.register_variant_id, rvar.name AS variant_name, "
        "vs.valid_from, vs.valid_to "
        "FROM register_variant rvar "
        "JOIN variable_state vs ON vs.register_variant_id = rvar.register_variant_id "
        "WHERE rvar.register_id = ? "
        "ORDER BY rvar.register_variant_id, vs.valid_from",
        (reg_id,),
    ).fetchall()

    all_years: set[int] = set()
    variants: dict[int, dict[str, Any]] = {}

    for row in rows:
        rvid = row["register_variant_id"]
        for year in _years_in_range(row["valid_from"], row["valid_to"]):
            all_years.add(year)
            if rvid not in variants:
                variants[rvid] = {
                    "register_variant_id": rvid,
                    "variant_name": row["variant_name"],
                    "years": [],
                }
            variants[rvid]["years"].append(year)

    for v in variants.values():
        v["years"] = sorted(set(v["years"]))

    if not all_years:
        return None

    all_years_sorted = sorted(all_years)
    min_y, max_y = all_years_sorted[0], all_years_sorted[-1]
    expected = set(range(min_y, max_y + 1))
    gaps = sorted(expected - all_years)

    return {
        "target": register,
        "target_type": "register",
        "register_id": reg_id,
        "register_name": reg["name"],
        "min_year": min_y,
        "max_year": max_y,
        "years": all_years_sorted,
        "gaps": gaps,
        "variant_count": len(variants),
        "variants": list(variants.values()),
    }


# ---------------------------------------------------------------------------
# Get values
# ---------------------------------------------------------------------------


def get_values_by_variable(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
    year: int | None = None,
) -> dict[str, Any]:
    """Resolve a variable to its states and return year-correct codes per state.

    A2.6: each "instance" is one `variable_state` → year-correct value list (the
    state's `value_set_id`). Filter via ``register`` and/or ``year``. Returns
    ``{input, variable_name, instances: [{state_id, register_id, register_name,
    register_variant_id, variant_name, valid_from, valid_to, year, values}]}``.
    Resolution mirrors ``get_varinfo``: var_id → variable name → alias.
    Keys follow the glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): `variabelnamn` → `variable_name`.
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    int_variable: int | None
    raw_int = _try_int(variable)
    int_variable = raw_int if isinstance(raw_int, int) else None

    # Carry `variable_id` from the match: it's the unique per-variable key the
    # state query filters by. `provider_key` (= var_id) is NON-unique after an
    # A2.2 split, so selecting states by it would merge sibling value sets.
    rows_by_id: list[Any] = []
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        if int_variable is not None:
            rows_by_id = conn.execute(
                "SELECT variable_id, register_id, "
                + _VAR_ID_BARE
                + ", name FROM variable "
                f"WHERE provider_key = CAST(? AS TEXT) AND register_id IN ({ph})",
                [int_variable, *reg_ids],
            ).fetchall()
        rows_by_name = conn.execute(
            "SELECT variable_id, register_id, " + _VAR_ID_BARE + ", name FROM variable "
            f"WHERE LOWER(name) = LOWER(?) AND register_id IN ({ph})",
            [variable, *reg_ids],
        ).fetchall()
    else:
        if int_variable is not None:
            rows_by_id = conn.execute(
                "SELECT variable_id, register_id, "
                + _VAR_ID_BARE
                + ", name FROM variable WHERE provider_key = CAST(? AS TEXT)",
                (int_variable,),
            ).fetchall()
        rows_by_name = conn.execute(
            "SELECT variable_id, register_id, " + _VAR_ID_BARE + ", name FROM variable "
            "WHERE LOWER(name) = LOWER(?)",
            (variable,),
        ).fetchall()

    matched = rows_by_id or rows_by_name

    if not matched:
        # A2.7: `variable_alias` is variable_id-keyed; join straight to `variable`.
        alias_sql = (
            "SELECT DISTINCT v.variable_id, v.register_id, " + _VAR_ID_V + ", v.name "
            "FROM variable_alias a "
            "JOIN variable v ON a.variable_id = v.variable_id "
            "WHERE LOWER(a.delivery_column_name) = LOWER(?)"
        )
        if reg_ids:
            ph = _in_placeholders(reg_ids)
            alias_sql += f" AND v.register_id IN ({ph})"
            matched = conn.execute(alias_sql, [variable, *reg_ids]).fetchall()
        else:
            matched = conn.execute(alias_sql, (variable,)).fetchall()

        # Generic column aliases (e.g. "Rad", "Kolumn1", "OBS_VALUE") map to
        # many unrelated variables. Refuse to silently merge their value sets
        # under one name — surface the spread so the caller can pick.
        distinct_names = {m["name"] for m in matched}
        if len(distinct_names) > 1:
            sample = ", ".join(sorted(distinct_names)[:5])
            more = (
                f" (+{len(distinct_names) - 5} more)" if len(distinct_names) > 5 else ""
            )
            raise RegMetaError(
                exit_code=EXIT_USAGE,
                code="ambiguous_alias",
                error_class="usage",
                message=(
                    f"Column alias '{variable}' maps to "
                    f"{len(distinct_names)} distinct variables: {sample}{more}."
                ),
                remediation=(
                    f"Run `reg-meta get datacolumns {variable}` to see the spread, "
                    "then call `reg-meta get values <name> --register R`."
                ),
            )

    if not matched:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `reg-meta search --query <term>` to find variables.",
        )

    variable_name = matched[0]["name"]

    # A2.6: "instances" are `variable_state` rows now (year from validity
    # window), not per-cvid `variable_instance` × `register_version` rows. Codes
    # come from the state's `value_set_id` (the same year-projected set the
    # coalescer assigned). Batched across all matched `variable_id`s to avoid the
    # N+1 pattern when a variable spans dozens of registers.
    #
    # Filter by `variable_id`, NOT `(register_id, provider_key)`: `provider_key`
    # is NON-unique after an A2.2 split (siblings share one source key), so a
    # provider_key filter would merge every sibling's value sets under one name.
    variable_ids = [var["variable_id"] for var in matched]
    vid_ph = _in_placeholders(variable_ids)

    state_rows = conn.execute(
        "SELECT vs.state_id, vs.value_set_id, vs.valid_from, vs.valid_to, "
        "vs.variable_id, v.slug AS variable_slug, "
        "v.register_id, " + _VAR_ID_V + ", "
        f"vs.register_variant_id, r.name AS register_name, rv.name AS variant_name "
        f"FROM variable_state vs "
        f"JOIN variable v ON vs.variable_id = v.variable_id "
        f"JOIN register r ON v.register_id = r.register_id "
        f"JOIN register_variant rv ON vs.register_variant_id = rv.register_variant_id "
        f"WHERE vs.variable_id IN ({vid_ph})",
        variable_ids,
    ).fetchall()

    instances: list[dict[str, Any]] = []
    # Group code rows by value_set_id; a state's `values` is its set's codes.
    by_value_set: dict[int, list[dict[str, Any]]] = {}
    for row in state_rows:
        # A2.6: a state matches the requested `year` when its validity window
        # COVERS that year (overlap), not only when the window opens in it — a
        # coalesced multi-year state (e.g. 2020-01-01..2021-12-31) must answer a
        # `--year 2021` query. `inst_year` (the opening year) stays for display.
        if year is not None and not _state_covers_year(
            row["valid_from"], row["valid_to"], year
        ):
            continue
        inst_year = int(row["valid_from"][:4])
        inst = {
            "state_id": row["state_id"],
            # A2.7: attribute each instance to its owning variable. A numeric
            # var_id can map to >1 A2.2 split sibling (same provider_key, distinct
            # variable_id/slug) with differing value sets — carrying the slug lets
            # the caller tell them apart instead of seeing them merged under one
            # name (Codex P2 #149).
            "variable_id": row["variable_id"],
            "variable_slug": row["variable_slug"],
            "register_id": row["register_id"],
            "register_name": row["register_name"],
            "register_variant_id": row["register_variant_id"],
            "variant_name": row["variant_name"],
            "valid_from": row["valid_from"],
            "valid_to": row["valid_to"],
            "year": inst_year,
            "values": [],
        }
        instances.append(inst)
        if row["value_set_id"] is not None:
            by_value_set.setdefault(row["value_set_id"], []).append(inst)

    if by_value_set:
        vs_ph = _in_placeholders(list(by_value_set))
        codes_by_set: dict[int, list[dict[str, Any]]] = {}
        for row in conn.execute(
            f"SELECT vsm.value_set_id, vc.code, vc.label "
            f"FROM value_set_member vsm "
            f"JOIN value_code vc ON vsm.code_id = vc.code_id "
            f"WHERE vsm.value_set_id IN ({vs_ph}) "
            f"ORDER BY vsm.value_set_id, vc.code",
            list(by_value_set),
        ):
            codes_by_set.setdefault(row["value_set_id"], []).append(
                {"code": row["code"], "label": row["label"]}
            )
        for vsid, insts in by_value_set.items():
            for inst in insts:
                inst["values"] = list(codes_by_set.get(vsid, []))

    instances.sort(
        key=lambda i: (i["register_name"] or "", i["year"] or 0, i["state_id"])
    )

    return {
        "input": variable,
        # Glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): surface as `variable_name` (variable.name with the entity
        # qualifier so consumers don't confuse it with register.name).
        "variable_name": variable_name,
        "instances": instances,
    }


# ---------------------------------------------------------------------------
# Resolve
# ---------------------------------------------------------------------------


def get_datacolumns(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
) -> list[dict[str, Any]]:
    """Get all delivery-column aliases for a variable.

    A2.7: sourced from `variable_alias` — the FULL delivery-column history,
    re-parented onto `variable_id` (was cvid-keyed through A2.6) and joined
    straight to `variable`. The full history is the right source here; the
    coalesced `variable_state.delivery_column_name` keeps only the denormalized
    latest era. Returns a list of dicts with "delivery_column_name",
    "register_id", "register_name", "register_variant_id". Keys follow the glossary rename (see DESIGN.md →
    Glossary and Swedish↔English crosswalk): `kolumnnamn` → `delivery_column_name`.

    Filters the alias rows by the matched `variable_id` (NOT the non-unique
    `(register_id, provider_key)`): an A2.2 split sibling has its own
    `variable_id`, so each sibling surfaces only its own columns.
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    # Match by var_id or variable name (glossary rename — was `variabelnamn`; see DESIGN.md → Glossary and Swedish↔English crosswalk). Carry
    # `variable_id` — the unique key the re-parented `variable_alias` filters by.
    int_variable = _try_int(variable)
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        var_rows = conn.execute(
            "SELECT variable_id, register_id, " + _VAR_ID_BARE + " FROM variable "
            f"WHERE (provider_key = CAST(? AS TEXT) OR LOWER(name) = LOWER(?)) "
            f"AND register_id IN ({ph})",
            [int_variable, variable, *reg_ids],
        ).fetchall()
    else:
        var_rows = conn.execute(
            "SELECT variable_id, register_id, " + _VAR_ID_BARE + " FROM variable "
            "WHERE provider_key = CAST(? AS TEXT) OR LOWER(name) = LOWER(?)",
            (int_variable, variable),
        ).fetchall()

    if not var_rows:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `reg-meta search` to find variable names or IDs.",
        )

    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for vr in var_rows:
        rows = conn.execute(
            "SELECT DISTINCT va.delivery_column_name, "
            "v.register_id, va.register_variant_id, r.name AS register_name "
            "FROM variable_alias va "
            "JOIN variable v ON va.variable_id = v.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE va.variable_id = ? "
            "ORDER BY va.delivery_column_name, va.register_variant_id",
            (vr["variable_id"],),
        ).fetchall()
        for r in rows:
            key = (
                f"{r['delivery_column_name']}:{r['register_id']}:"
                f"{r['register_variant_id']}"
            )
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    # Glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): SCB `kolumnnamn` → universal
                    # `delivery_column_name`.
                    "delivery_column_name": r["delivery_column_name"],
                    "register_id": r["register_id"],
                    "register_name": r["register_name"],
                    "register_variant_id": r["register_variant_id"],
                }
            )

    return results


# ---------------------------------------------------------------------------
# Get diff
# ---------------------------------------------------------------------------


def _columns_at_year(
    conn: sqlite3.Connection, register_variant_id: int, year: int
) -> dict[int, dict[str, Any]]:
    """A2.6: the variant's columns active at a calendar `year`, keyed by var_id.

    Sourced from `variable_state` validity windows (register_version is dropped
    before ship): a state is active at `year` when `valid_from`..`valid_to`
    overlaps that calendar year. Returns an empty dict when the variant has no
    state covering the year (the caller treats that like "version absent").

    Keyed by `variable_id` (#474): each variable is a distinct schema column, so
    one entry per variable. The DISPLAYED `var_id` (numeric for SCB, blank for
    non-SCB) rides on the row but is NOT the key — keying by it collapsed every
    non-SCB variable (shared NULL var_id) into one entry, dropping all but the
    first column from the diff. `variable_id` is always present and unique.

    A2.2 split siblings get distinct `variable_id`s, so they no longer collapse;
    each surfaces as its own column. (The old "lex-smallest column wins per
    var_id" de-dup is gone — it only collapsed siblings that share a var_id, which
    was the bug, not a feature.)
    """
    iso_lo = f"{year:04d}-12-31"  # any state starting on/before year-end ...
    iso_hi = f"{year:04d}-01-01"  # ... and ending on/after year-start overlaps
    rows = conn.execute(
        "SELECT v.variable_id, " + _VAR_ID_V + ", vs.data_type, "
        "vs.data_length, v.name AS variable_name, vs.delivery_column_name "
        "FROM variable_state vs "
        "JOIN variable v ON vs.variable_id = v.variable_id "
        "WHERE vs.register_variant_id = ? "
        "AND vs.valid_from <= ? AND vs.valid_to >= ? "
        "ORDER BY v.provider_key, vs.delivery_column_name",
        (register_variant_id, iso_lo, iso_hi),
    ).fetchall()
    result: dict[int, dict[str, Any]] = {}
    for r in rows:
        if r["variable_id"] in result:
            continue  # one row per variable; first (lex-smallest column) wins
        col = r["delivery_column_name"]
        result[r["variable_id"]] = {
            "variable_id": r["variable_id"],
            "var_id": r["var_id"],
            "variable_name": r["variable_name"],
            "data_type": r["data_type"],
            "data_length": r["data_length"],
            "aliases": [col] if col else [],
        }
    return result


def get_diff(
    conn: sqlite3.Connection,
    *,
    register: str,
    from_year: int,
    to_year: int,
    variant: str | None = None,
    variables: list[str] | None = None,
) -> dict[str, Any]:
    """Compare a register's schema between two years."""
    reg_ids = require_register_ids(conn, register)

    reg = conn.execute(
        "SELECT register_id, name FROM register WHERE register_id = ?",
        (reg_ids[0],),
    ).fetchone()

    if variant:
        variant_rows = conn.execute(
            f"SELECT * FROM register_variant WHERE register_id IN ({_in_placeholders(reg_ids)}) "
            "AND register_variant_id = ?",
            [*reg_ids, _try_int(variant)],
        ).fetchall()
    else:
        variant_rows = conn.execute(
            f"SELECT * FROM register_variant WHERE register_id IN ({_in_placeholders(reg_ids)}) "
            "ORDER BY register_variant_id",
            reg_ids,
        ).fetchall()

    # Resolve each variable input to `variable_id`s (#474 — the unique key the
    # diff is keyed by; the displayed `var_id` rides alongside for output but is
    # NOT the key, since it collapses non-SCB variables under one NULL).
    filter_variable_ids: set[int] | None = None
    var_name_by_id: dict[int, str] = {}
    var_input_by_id: dict[int, str] = {}
    var_id_display_by_id: dict[int, int | None] = {}
    if variables:
        filter_variable_ids = set()
        ph = _in_placeholders(reg_ids)
        for v in variables:
            rows = conn.execute(
                "SELECT variable_id, " + _VAR_ID_BARE + ", name FROM variable "
                f"WHERE (provider_key = CAST(? AS TEXT) OR LOWER(name) = LOWER(?)) "
                f"AND register_id IN ({ph})",
                [_try_int(v), v, *reg_ids],
            ).fetchall()
            if not rows:
                # A2.7: `variable_alias` is variable_id-keyed; join straight to
                # `variable`. `var_id` is the variable's `provider_key`.
                rows = conn.execute(
                    "SELECT DISTINCT var.variable_id, " + _VAR_ID_VAR + ", var.name "
                    f"FROM variable_alias va "
                    f"JOIN variable var ON va.variable_id = var.variable_id "
                    f"WHERE LOWER(va.delivery_column_name) = LOWER(?) AND var.register_id IN ({ph})",
                    [v, *reg_ids],
                ).fetchall()
            for r in rows:
                filter_variable_ids.add(r["variable_id"])
                var_name_by_id[r["variable_id"]] = r["name"]
                var_input_by_id[r["variable_id"]] = v
                var_id_display_by_id[r["variable_id"]] = r["var_id"]

        if not filter_variable_ids:
            names = ", ".join(f"'{v}'" for v in variables)
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"No variables matching {names} in register '{register}'.",
                remediation="Use `reg-meta search --query <term>` to find variables.",
            )

    variants_out: list[dict[str, Any]] = []
    # Keyed by `variable_id` (#474), matching `_columns_at_year`'s dict keys.
    unchanged_by_var: dict[int, list[str]] = {}
    changed_any_variant: set[int] = set()
    any_versions_found = False

    for rv in variant_rows:
        rvid = rv["register_variant_id"]
        # A2.6: the columns active in each year come from `variable_state`
        # validity windows (register_version is dropped before ship). A variant
        # with no state covering a year contributes nothing — same skip as the
        # old "version absent" branch.
        from_cols = _columns_at_year(conn, rvid, from_year)
        to_cols = _columns_at_year(conn, rvid, to_year)
        if not from_cols or not to_cols:
            continue
        any_versions_found = True

        # `*_cols` are keyed by `variable_id` (#474), so the set arithmetic and
        # all the `*_ids` below are variable_ids — the displayed `var_id` is
        # carried on each row for output but is never the key.
        from_ids = set(from_cols)
        to_ids = set(to_cols)

        added_ids = to_ids - from_ids
        removed_ids = from_ids - to_ids
        common_ids = from_ids & to_ids

        added = [to_cols[vid] for vid in sorted(added_ids)]
        removed = [from_cols[vid] for vid in sorted(removed_ids)]
        changed: list[dict[str, Any]] = []
        unchanged_count = 0

        for vid in sorted(common_ids):
            fc, tc = from_cols[vid], to_cols[vid]
            changes: list[dict[str, Any]] = []
            for field in ("data_type", "data_length", "aliases"):
                if fc[field] != tc[field]:
                    changes.append({"field": field, "from": fc[field], "to": tc[field]})
            if changes:
                changed.append(
                    {
                        "variable_id": vid,
                        "var_id": tc["var_id"],
                        "variable_name": tc["variable_name"],
                        "changes": changes,
                    }
                )
            else:
                unchanged_count += 1

        if filter_variable_ids is not None:
            changed_variable_ids = (
                {a["variable_id"] for a in added}
                | {r["variable_id"] for r in removed}
                | {c["variable_id"] for c in changed}
            ) & filter_variable_ids
            changed_any_variant.update(changed_variable_ids)
            for vid in filter_variable_ids - changed_variable_ids:
                if vid in from_ids or vid in to_ids:
                    # `name` is the glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk) of
                    # `registervariantnamn` on register_variant.
                    unchanged_by_var.setdefault(vid, []).append(rv["name"])

            added = [a for a in added if a["variable_id"] in filter_variable_ids]
            removed = [r for r in removed if r["variable_id"] in filter_variable_ids]
            changed = [c for c in changed if c["variable_id"] in filter_variable_ids]

        if not added and not removed and not changed:
            continue

        variants_out.append(
            {
                "register_variant_id": rvid,
                "variant_name": rv["name"],
                # A2.6: the diff is year-keyed (no register_version rows to name).
                "from_year": from_year,
                "to_year": to_year,
                "summary": {
                    "added": len(added),
                    "removed": len(removed),
                    "changed": len(changed),
                    "unchanged": unchanged_count,
                },
                "added": added,
                "removed": removed,
                "changed": changed,
            }
        )

    if not any_versions_found:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No versions found for register '{register}' between years {from_year} and {to_year}.",
            remediation="Use `reg-meta get schema --register <name>` to see available versions.",
        )

    result: dict[str, Any] = {
        "register_id": reg["register_id"],
        "register_name": reg["name"],
        "from_year": from_year,
        "to_year": to_year,
        "variants": variants_out,
    }
    if var_input_by_id:
        # Keyed/sorted by `variable_id` internally; the displayed `var_id` (numeric
        # for SCB, None for non-SCB) is the carried display value, not the key.
        result["resolved_variables"] = [
            {
                "input": var_input_by_id[vid],
                "variable_name": var_name_by_id[vid],
                "var_id": var_id_display_by_id[vid],
            }
            for vid in sorted(var_name_by_id)
        ]
    fully_unchanged = [
        var_name_by_id[vid]
        for vid in sorted(unchanged_by_var)
        if vid not in changed_any_variant
    ]
    if fully_unchanged:
        result["unchanged"] = fully_unchanged
    return result


# ---------------------------------------------------------------------------
# Get lineage
# ---------------------------------------------------------------------------


def get_lineage(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
) -> dict[str, Any]:
    """Show cross-register variable provenance."""
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    int_variable = _try_int(variable)
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        matched = conn.execute(
            "SELECT v.*, " + _VAR_ID_V + ", r.name AS register_name FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE (v.provider_key = CAST(? AS TEXT) OR LOWER(v.name) = LOWER(?)) "
            f"AND v.register_id IN ({ph})",
            [int_variable, variable, *reg_ids],
        ).fetchall()
    else:
        matched = conn.execute(
            "SELECT v.*, " + _VAR_ID_V + ", r.name AS register_name FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE v.provider_key = CAST(? AS TEXT) OR LOWER(v.name) = LOWER(?)",
            (int_variable, variable),
        ).fetchall()

    if not matched:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `reg-meta search --query <term>` to find variables.",
        )

    registers_out: list[dict[str, Any]] = []
    total_instances = 0
    with_source = 0

    for var in matched:
        rid, vid = var["register_id"], var["var_id"]
        variable_id = var["variable_id"]
        # Glossary-rename drop (see DESIGN.md → Glossary and Swedish↔English crosswalk): `variabelhamtadfran` is no longer ingested. Lineage
        # role detection now keys solely on `source_register_text` (the
        # renamed `variabelregister_kalla`); the auxiliary `hamtad` text
        # carried no orthogonal signal in practice and its disposition is
        # "(dropped)" per the same glossary rename.
        kalla = (var["source_register_text"] or "").strip()
        source_register_id = var["source_register_id"]

        # Classify role
        if not kalla:
            role = "unknown"
        elif source_register_id != rid:
            role = "consumer"
        else:
            role = "source"

        # A2.6: state count + year range from `variable_state` (register_version
        # is dropped before ship). `instance_count` counts states now (the
        # per-delivery shape), not per-cvid `variable_instance` rows.
        #
        # Select by the matched `variable_id`, NOT `(register_id, provider_key)`:
        # `matched` already yields one row per split sibling (each with its own
        # `variable_id` / role), so the state count must be per-sibling — a
        # provider_key filter is NON-unique post-split and would sum siblings.
        states = conn.execute(
            "SELECT vs.valid_from, vs.valid_to FROM variable_state vs "
            "WHERE vs.variable_id = ?",
            (variable_id,),
        ).fetchall()

        instance_count = len(states)
        years = [
            y for s in states for y in _years_in_range(s["valid_from"], s["valid_to"])
        ]
        year_range = [min(years), max(years)] if years else []

        total_instances += instance_count
        if kalla:
            with_source += instance_count

        registers_out.append(
            {
                "register_id": rid,
                "register_name": var["register_name"],
                "var_id": vid,
                "role": role,
                # Glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): surface SCB's raw attribution under the universal
                # English key. `variabelhamtadfran` was dropped at the same rename; lineage
                # signal collapsed onto `source_register_text` alone.
                "source_register_text": kalla,
                "source_register_id": source_register_id,
                "instance_count": instance_count,
                "year_range": year_range,
            }
        )

    var_name = matched[0]["name"]

    return {
        "variable_name": var_name,
        "occurrences": total_instances,
        "registers": registers_out,
        "provenance_coverage": {
            "total": total_instances,
            "with_source": with_source,
            "without_source": total_instances - with_source,
        },
    }


# ---------------------------------------------------------------------------
# Resolve
# ---------------------------------------------------------------------------


def get_coded_variables(
    conn: sqlite3.Connection,
    *,
    min_codes: int = 1,
    min_registers: int = 1,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Find variables that have value sets, ranked by usage.

    Returns a list of dicts with "variable_name", "n_distinct_codes",
    "n_registers", "n_instances".

    A2.7: sourced from `variable_state` (was per-cvid `variable_instance`).
    `n_instances` counts distinct states now — the per-era shape is the unit the
    shipped DB carries.
    """
    rows = conn.execute(
        "SELECT v.name AS variable_name, "
        "COUNT(DISTINCT vc.code) as n_distinct_codes, "
        "COUNT(DISTINCT v.register_id) as n_registers, "
        "COUNT(DISTINCT vs.state_id) as n_instances "
        "FROM variable v "
        "JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "JOIN value_set_member vsm ON vs.value_set_id = vsm.value_set_id "
        "JOIN value_code vc ON vsm.code_id = vc.code_id "
        "GROUP BY v.name "
        "HAVING n_distinct_codes >= ? AND n_registers >= ? "
        "ORDER BY n_registers DESC, n_distinct_codes DESC "
        "LIMIT ?",
        (min_codes, min_registers, limit),
    ).fetchall()
    return [
        {
            "variable_name": r["variable_name"],
            "n_distinct_codes": r["n_distinct_codes"],
            "n_registers": r["n_registers"],
            "n_instances": r["n_instances"],
        }
        for r in rows
    ]


def resolve(
    conn: sqlite3.Connection,
    columns: list[str],
    *,
    register: str | None = None,
) -> list[dict[str, Any]]:
    """Resolve column names to variables via exact alias lookup.

    Returns a list of dicts, one per input column, each with
    "column_name", "status", and "matches" keys.
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    results: list[dict[str, Any]] = []

    for col in columns:
        col_lower = col.lower()

        # A2.7: `variable_alias` is variable_id-keyed; join straight to
        # `variable`. `var_id` is the variable's `provider_key`.
        if reg_ids:
            ph = _in_placeholders(reg_ids)
            exact_rows = conn.execute(
                "SELECT va.delivery_column_name, v.register_id, "
                "" + _VAR_ID_V + ", v.name AS variable_name "
                f"FROM variable_alias va "
                f"JOIN variable v ON va.variable_id = v.variable_id "
                f"WHERE LOWER(va.delivery_column_name) = ? AND v.register_id IN ({ph}) "
                f"GROUP BY v.register_id, v.provider_key "
                f"ORDER BY v.register_id, v.provider_key",
                [col_lower, *reg_ids],
            ).fetchall()
        else:
            exact_rows = conn.execute(
                "SELECT va.delivery_column_name, v.register_id, "
                "" + _VAR_ID_V + ", v.name AS variable_name "
                "FROM variable_alias va "
                "JOIN variable v ON va.variable_id = v.variable_id "
                "WHERE LOWER(va.delivery_column_name) = ? "
                "GROUP BY v.register_id, v.provider_key "
                "ORDER BY v.register_id, v.provider_key",
                (col_lower,),
            ).fetchall()

        matches = [
            {
                "var_id": r["var_id"],
                "variable_name": r["variable_name"],
                "matched_column": r["delivery_column_name"],
                "register_id": r["register_id"],
            }
            for r in exact_rows
        ]

        results.append(
            {
                "column_name": col,
                "status": "matched" if matches else "no_match",
                "matches": matches,
            }
        )

    return results


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------


def compare(
    conn: sqlite3.Connection,
    *,
    columns_by_file: dict[str, list[str]],
    register_hints: dict[str, int | None] | None = None,
    year_hints: dict[str, int | None] | None = None,
) -> dict[str, Any]:
    """Compare local file columns against registry metadata.

    For each file (keyed by label), resolves the register (from hint or
    explicit), retrieves the registry schema, and classifies columns as
    matched, extra_local, or missing_from_registry.
    """
    register_hints = register_hints or {}
    year_hints = year_hints or {}

    files_out: list[dict[str, Any]] = []

    for file_label, local_columns in columns_by_file.items():
        reg_hint = register_hints.get(file_label)
        year_hint = year_hints.get(file_label)

        if reg_hint is None:
            files_out.append(
                {
                    "file": file_label,
                    "register_id": None,
                    "register_name": None,
                    "register_status": "no_hint",
                    "year_hint": year_hint,
                    "matched": [],
                    "extra_local": local_columns,
                    "missing_from_registry": [],
                    "summary": {
                        "matched": 0,
                        "extra_local": len(local_columns),
                        "missing_from_registry": 0,
                    },
                }
            )
            continue

        # Resolve register_id to register name
        reg_row = conn.execute(
            "SELECT name FROM register WHERE register_id = ?", (reg_hint,)
        ).fetchone()
        if not reg_row:
            files_out.append(
                {
                    "file": file_label,
                    "register_id": reg_hint,
                    "register_name": None,
                    "register_status": "not_found",
                    "year_hint": year_hint,
                    "matched": [],
                    "extra_local": local_columns,
                    "missing_from_registry": [],
                    "summary": {
                        "matched": 0,
                        "extra_local": len(local_columns),
                        "missing_from_registry": 0,
                    },
                }
            )
            continue

        register_name = reg_row["name"]

        # Get schema for this register, optionally filtered by year
        years_arg = str(year_hint) if year_hint else None
        try:
            schema = get_schema(conn, register=str(reg_hint), years=years_arg)
        except RegMetaError:
            files_out.append(
                {
                    "file": file_label,
                    "register_id": reg_hint,
                    "register_name": register_name,
                    "register_status": "no_schema",
                    "year_hint": year_hint,
                    "matched": [],
                    "extra_local": local_columns,
                    "missing_from_registry": [],
                    "summary": {
                        "matched": 0,
                        "extra_local": len(local_columns),
                        "missing_from_registry": 0,
                    },
                }
            )
            continue

        # Flatten schema: build alias→variable mapping.
        # Keyed by `variable_id` (#474) — the displayed `var_id` is NULL for every
        # non-SCB column, so keying `all_registry_vars` / `matched_var_ids` by it
        # collapsed every non-SCB registry column into one entry (matching one
        # local column then suppressed all the others as "matched"). `variable_id`
        # is always present and unique; `var_id` rides along for display.
        alias_to_var: dict[str, dict[str, Any]] = {}
        all_registry_vars: dict[int, dict[str, Any]] = {}
        for variant in schema.get("variants", []):
            for version in variant.get("versions", []):
                for col in version.get("columns", []):
                    variable_id = col["variable_id"]
                    vid = col["var_id"]
                    vname = col["variable_name"]
                    aliases_str = col.get("aliases") or ""
                    aliases = [a.strip() for a in aliases_str.split(",") if a.strip()]

                    var_info = {
                        "variable_id": variable_id,
                        "var_id": vid,
                        "variable_name": vname,
                        "aliases": aliases,
                    }
                    all_registry_vars[variable_id] = var_info

                    for alias in aliases:
                        alias_to_var[alias.lower()] = var_info
                    alias_to_var[vname.lower()] = var_info

        # Classify local columns
        matched = []
        extra_local = []
        matched_variable_ids: set[int] = set()
        local_lower = set()

        for col in local_columns:
            col_lower = col.lower()
            local_lower.add(col_lower)
            var_info = alias_to_var.get(col_lower)
            if var_info:
                matched.append(
                    {
                        "column": col,
                        "var_id": var_info["var_id"],
                        "variable_name": var_info["variable_name"],
                    }
                )
                matched_variable_ids.add(var_info["variable_id"])
            else:
                extra_local.append(col)

        # Registry variables not in local columns
        missing_from_registry = []
        for variable_id, var_info in sorted(all_registry_vars.items()):
            if variable_id in matched_variable_ids:
                continue
            if any(a.lower() in local_lower for a in var_info["aliases"]):
                continue
            if var_info["variable_name"].lower() in local_lower:
                continue
            missing_from_registry.append(
                {
                    "var_id": var_info["var_id"],
                    "variable_name": var_info["variable_name"],
                    "aliases": var_info["aliases"],
                }
            )

        files_out.append(
            {
                "file": file_label,
                "register_id": reg_hint,
                "register_name": register_name,
                "register_status": "resolved",
                "year_hint": year_hint,
                "matched": matched,
                "extra_local": extra_local,
                "missing_from_registry": missing_from_registry,
                "summary": {
                    "matched": len(matched),
                    "extra_local": len(extra_local),
                    "missing_from_registry": len(missing_from_registry),
                },
            }
        )

    return {"files": files_out}


# ---------------------------------------------------------------------------
# Classifications
# ---------------------------------------------------------------------------


def _classification_row(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    fqid = try_emit(Fqid.classification_fqid, d.get("slug"))
    # Drop NULL fields to keep JSON output lean.
    out = {k: v for k, v in d.items() if v is not None}
    if fqid:
        out["fqid"] = fqid
    return out


def list_classifications(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Enumerate all classifications with a superseded_by back-pointer.

    superseded_by uses a scalar GROUP_CONCAT subquery rather than a LEFT JOIN
    on supersedes_id: a classification can be superseded by more than one
    successor (the schema doesn't enforce 1:1), and a JOIN would multiply
    the parent row. The result is comma-separated when there are multiple.
    """
    rows = conn.execute(
        """
        SELECT c.id, c.short_name, c.slug, c.name, c.name_en, c.publisher,
               c.valid_from, c.valid_to, c.description, c.url, c.code_count,
               c.valid_code_count,
               s.short_name AS supersedes,
               (SELECT GROUP_CONCAT(short_name, ',')
                FROM (SELECT short_name FROM classification
                      WHERE supersedes_id = c.id ORDER BY short_name)) AS superseded_by
        FROM classification c
        LEFT JOIN classification s ON c.supersedes_id = s.id
        ORDER BY c.short_name
        """
    ).fetchall()
    return [_classification_row(r) for r in rows]


def _resolve_classification_id(conn: sqlite3.Connection, value: str) -> int:
    """Resolve a classification by id, short_name (case-insensitive), or substring."""
    int_value = _try_int(value)
    if isinstance(int_value, int):
        row = conn.execute(
            "SELECT id FROM classification WHERE id = ?", (int_value,)
        ).fetchone()
        if row:
            return row["id"]

    row = conn.execute(
        "SELECT id FROM classification WHERE LOWER(short_name) = LOWER(?)",
        (value,),
    ).fetchone()
    if row:
        return row["id"]

    rows = conn.execute(
        "SELECT id, short_name FROM classification "
        "WHERE LOWER(short_name) LIKE '%' || LOWER(?) || '%' "
        "   OR LOWER(name) LIKE '%' || LOWER(?) || '%' "
        "ORDER BY short_name",
        (value, value),
    ).fetchall()
    if len(rows) == 1:
        return rows[0]["id"]
    if len(rows) > 1:
        candidates = ", ".join(r["short_name"] for r in rows)
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="ambiguous",
            error_class="query",
            message=(f"Classification {value!r} is ambiguous: matches {candidates}."),
            remediation="Use the exact short_name.",
        )
    raise RegMetaError(
        exit_code=EXIT_NOT_FOUND,
        code="not_found",
        error_class="query",
        message=f"No classification matching '{value}'.",
        remediation="Use `reg-meta get classification --list` to see available classifications.",
    )


def _classification_by_id(conn: sqlite3.Connection, cls_id: int) -> dict[str, Any]:
    # Scalar GROUP_CONCAT for superseded_by — see list_classifications for why.
    row = conn.execute(
        """
        SELECT c.*, s.short_name AS supersedes,
               (SELECT GROUP_CONCAT(short_name, ',')
                FROM (SELECT short_name FROM classification
                      WHERE supersedes_id = c.id ORDER BY short_name)) AS superseded_by
        FROM classification c
        LEFT JOIN classification s ON c.supersedes_id = s.id
        WHERE c.id = ?
        """,
        (cls_id,),
    ).fetchone()
    data = _classification_row(row)
    # supersedes_id is an internal FK; supersedes short_name is the useful form.
    data.pop("supersedes_id", None)
    return data


def get_classification(conn: sqlite3.Connection, identifier: str) -> dict[str, Any]:
    """Return one classification's metadata (no codes)."""
    return _classification_by_id(conn, _resolve_classification_id(conn, identifier))


def get_classification_codes(
    conn: sqlite3.Connection,
    identifier: str,
    *,
    level: int | None = None,
    only_valid: bool = False,
) -> dict[str, Any]:
    """Return a classification plus its full code list (optionally filtered).

    With ``only_valid=True`` the result only includes codes flagged as
    canonical (``is_valid=1``). Classifications without a canonical CSV have
    ``is_valid=NULL`` everywhere; ``only_valid`` will return zero codes for
    them, which is the correct semantics ("no canonical list available").
    """
    cls_id = _resolve_classification_id(conn, identifier)
    meta = _classification_by_id(conn, cls_id)

    sql = (
        "SELECT vc.code, vc.label, cc.level, cc.is_valid "
        "FROM classification_code cc "
        "JOIN value_code vc ON cc.code_id = vc.code_id "
        "WHERE cc.classification_id = ?"
    )
    params: list[Any] = [cls_id]
    if level is not None:
        sql += " AND cc.level = ?"
        params.append(level)
    if only_valid:
        sql += " AND cc.is_valid = 1"
    sql += " ORDER BY vc.code"

    # Strip is_valid when NULL so classifications without a canonical CSV
    # don't carry a meaningless field on every code.
    codes = []
    for r in conn.execute(sql, params).fetchall():
        row = dict(r)
        if row["is_valid"] is None:
            del row["is_valid"]
        codes.append(row)
    meta["codes"] = codes
    return meta


def search_variables_by_classification(
    conn: sqlite3.Connection,
    identifier: str,
    *,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """List variables with at least one state tagged with this classification.

    A2.7: re-sourced off `variable_state.classification_id` (was per-instance
    `variable_instance.classification_id`). `variable_state` carries
    `variable_id`, so the join is direct and sibling-isolated.
    """
    cls_id = _resolve_classification_id(conn, identifier)
    rows = conn.execute(
        f"""
        SELECT DISTINCT v.register_id, r.name AS register_name,
               {_VAR_ID_V}, v.name AS variable_name
        FROM variable_state vs
        JOIN variable v ON vs.variable_id = v.variable_id
        JOIN register r ON v.register_id = r.register_id
        WHERE vs.classification_id = ?
        ORDER BY r.name, v.name
        LIMIT ? OFFSET ?
        """,
        (cls_id, limit, offset),
    ).fetchall()
    return [dict(r) for r in rows]


def classifications_for_variable(
    conn: sqlite3.Connection, variable_id: int
) -> list[dict[str, Any]]:
    """Return the distinct classifications a variable's states use.

    A single variable can span multiple classifications across its lifetime
    (e.g. SUN2000 → SUN2020), so this returns a list, not a scalar.

    A2.7: re-sourced off `variable_state.classification_id` and keyed by
    `variable_id` (the unique per-variable key). This SIBLING-ISOLATES — the
    A2.6 limitation (where `variable_instance` had no `variable_id`, so an A2.2
    split sibling's classifications aggregated across every sibling sharing the
    `var_id`) is resolved. `instance_count` counts distinct states now.
    """
    rows = conn.execute(
        """
        SELECT c.id, c.short_name, c.name, c.publisher,
               COUNT(DISTINCT vs.state_id) AS instance_count
        FROM variable_state vs
        JOIN classification c ON vs.classification_id = c.id
        WHERE vs.variable_id = ?
        GROUP BY c.id
        ORDER BY c.short_name
        """,
        (variable_id,),
    ).fetchall()
    return [dict(r) for r in rows]
