"""`GET /api/search` — global catalog search over the shipped FTS5 indexes.

Typed result groups (`registers` / `variables` / `classifications`) over
reg_meta's FTS indexes, reusing reg_meta's concept-group-folded search
(`reg_meta.queries.search`, #322) — the webapp does NOT reimplement folding or
FTS. See DESIGN.md → Global catalog search for the contract and extension seam.

Connection model = the shared per-request open (``reg_webapp.conn.catalog_conn``),
same as the catalog routes. The query string is part of the URL, so the ETag
middleware's content-derived ETag already covers it (and the CF edge keys by full
URL) — no caching work here. Query-side input hardening (length cap, NUL reject,
FTS-operator neutralization) lives in the gates below; the query reaches FTS only
as a bound parameter.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, Depends, HTTPException, Request
from reg_meta.queries import SEARCH_TYPES, search as reg_meta_search

from reg_webapp import golden
from reg_webapp.conn import catalog_conn
from reg_webapp.models import (
    ClassificationSearchGroup,
    CodeSearchGroup,
    RegisterSearchGroup,
    SearchGroup,
    SearchResponse,
    VariableSearchGroup,
)
from reg_webapp.query_input import validate_text_query

if TYPE_CHECKING:
    from reg_meta.search import CodeSearchResult, RegisterSearchResult, SearchResult

    from reg_webapp.models import ClassificationSearchItem, VariableSearchItem

router = APIRouter(prefix="/api")

_DEFAULT_LIMIT = 20
_MAX_LIMIT = 50

# A "real" token carries at least one unicode alphanumeric char; pure
# punctuation tokenizes to nothing in FTS5 and would yield an empty phrase.
_WORD_CHAR = re.compile(r"[^\W_]", re.UNICODE)


def _validated_limit(limit: int = _DEFAULT_LIMIT) -> int:
    """``?limit`` gate: per-group display cap. Clamped to [1, _MAX_LIMIT] rather
    than 422'd — a researcher nudging the number shouldn't hit an error wall."""
    return max(1, min(limit, _MAX_LIMIT))


def _validated_type(type: str = "all") -> str:
    """``?type`` gate: scope the search to a single result group (#393 item 1).
    The param is named ``type`` so the wire param is ``?type=``; the bound name in
    `get_search` is `req_type` to avoid shadowing the `type` builtin. An unknown
    value 422s here rather than reaching reg_meta (where a bad `type` raises
    `RegMetaError` → 500) — fail fast at the boundary with the valid set."""
    if type not in SEARCH_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"invalid type '{type}' (valid: {sorted(SEARCH_TYPES)})",
        )
    return type


def _has_searchable_token(q: str) -> bool:
    """Whether the query carries at least one unicode alphanumeric char. A
    blank / whitespace / punctuation-only query short-circuits to empty groups
    WITHOUT hitting reg_meta — the FTS path would no-op anyway, but reg_meta's
    LIKE-based group-label fold uses a raw `%query%` pattern that an empty query
    turns into `%%` (matches every label). FTS quoting/prefixing itself lives in
    reg_meta (`_fts_match_query`) — the webapp passes the raw query through; FTS5
    folds diacritics on both index and query side (å→a), matching the SPA's
    ``foldText``."""
    return _WORD_CHAR.search(q) is not None


def _rank_codes(results: list[CodeSearchResult]) -> list[CodeSearchResult]:
    """Re-rank a code/value page so classification-backed (curated) codes lead,
    then by classification_count, then variable_count — all DESCENDING; FTS order
    is preserved within ties (stable sort, #393 item 2).

    LIMITATION (deferred annotation): reg_meta returns the FTS-top-N codes and only
    THEN annotates the shown page with owner counts (`_annotate_value_page`), so the
    counts exist only on the rows already in the page. This re-sort therefore only
    reorders WITHIN that page — it cannot pull a curated code that ranked below the
    FTS cutoff into view. Accepted as an easy-win until ranking moves into reg_meta.

    Operates on reg_meta's `CodeSearchResult` models directly (#701) — no per-row
    re-wrapping. Pure helper (no IO) so it's unit-testable in isolation."""
    return sorted(
        results,
        key=lambda r: (
            r.classification_count > 0,
            r.classification_count,
            r.variable_count,
        ),
        reverse=True,
    )


def _scope_to_fqids(
    results: list[SearchResult], fqids: frozenset[str] | None
) -> list[SearchResult]:
    """Drop register/variable results whose navigable `fqid` is not in the
    steward's held set (#859). A no-op when `fqids` is None (the `global`
    deployment — byte-identical to pre-#859). The reg_meta hits are already
    `fqids`-scoped query-time; this re-applies the same gate to the golden boost,
    which prepends pins from a separate source. Compares the SERIALIZED fqid string
    (the model's `fqid` is an `Fqid | None`; the held set holds canonical strings),
    mirroring `golden.apply_golden_boost`'s dedup; a result with `fqid is None` is
    unaddressable, so it can be in no steward catalog and drops."""
    if fqids is None:
        return results
    return [r for r in results if (f := getattr(r, "fqid", None)) and str(f) in fqids]


@router.get("/search", response_model=SearchResponse)
def get_search(
    request: Request,
    q: str = Depends(validate_text_query),
    limit: int = Depends(_validated_limit),
    req_type: str = Depends(_validated_type),
) -> SearchResponse:
    """Search registers, variables (concept-folded, #322), classifications, and
    codes/values (#352) over the shipped FTS indexes. Each group is an independent
    reg_meta `search()` call (register/variable/classification via the FTS
    `field="description"` path; codes via the `field="value"` path) so each carries
    its own `total_count` and per-group `limit`. A query with no usable token
    returns the selected group(s) empty (total 0) — not a 422.

    ``?type=`` (#393 item 1) scopes the search: ``all`` (the default) preserves the
    four-group register→variable→classification→code behavior; any single type runs
    AND emits only that one group. Group ORDER is fixed for the ``all`` case.

    A FILTERED steward (``app.state.catalog_index`` present, #859) scopes the
    REGISTER and VARIABLE surfaces to the steward's held FQIDs — both the reg_meta
    query (the ``fqids`` allow-list, applied query-time so ``total_count`` is exact)
    and the golden boost (a boosted pin the steward does not hold is dropped). The
    CLASSIFICATION and VALUE/code surfaces are catalog-global and pass through
    unscoped. The ``global`` deployment (no index) is byte-for-byte unchanged."""
    # Per-type gates: each arm runs (and its group is emitted) only when the
    # requested type selects it. `all` selects every arm.
    want_register = req_type in ("all", "register")
    want_variable = req_type in ("all", "variable")
    want_classification = req_type in ("all", "classification")
    want_value = req_type in ("all", "value")

    # #859: a filtered steward's held-FQID allow-list scopes the register/variable
    # surfaces (None for the `global` deployment → no restriction). The set is the
    # held registers UNIONED with the held binding FQIDs — a register hit matches on
    # its 2-seg FQID, a variable hit on its 3-seg binding FQID.
    index = request.app.state.catalog_index
    fqids = (
        index.held_register_fqids | index.admitted_variable_fqids
        if index is not None
        else None
    )

    # Groups are appended in the fixed register→variable→classification→code order
    # so the `all` case keeps today's exact 4-group shape; a single-type scope emits
    # just its one group.
    groups: list[SearchGroup] = []

    # The empty-query / no-usable-token short-circuit: same group selection as the
    # live path, just empty.
    if not _has_searchable_token(q):
        if want_register:
            groups.append(RegisterSearchGroup(total_count=0, results=[]))
        if want_variable:
            groups.append(VariableSearchGroup(total_count=0, results=[]))
        if want_classification:
            groups.append(ClassificationSearchGroup(total_count=0, results=[]))
        if want_value:
            groups.append(CodeSearchGroup(total_count=0, results=[]))
        return SearchResponse(query=q, groups=groups)

    with catalog_conn(request) as conn:
        # `field="description"` is reg_meta's FTS path (register_fts +
        # variable_fts + classification_fts) — NOT the LIKE-based
        # datacolumn/varname/value fields (codes are #352's own group). reg_meta
        # builds the safe FTS MATCH expression from the raw query internally; one
        # call per type yields a per-group total_count + limit. Registers have no
        # concept groups, so folding is off there.
        # reg_meta `search()` now returns typed `SearchResults` (#701): each arm's
        # rows are ALREADY the right reg_meta result models, so the webapp slices
        # and groups them directly — no per-row re-wrapping. The golden seam and the
        # FastAPI response models all operate on the same reg_meta types.
        if want_register:
            reg = reg_meta_search(
                conn,
                q,
                field="description",
                type="register",
                fqids=fqids,
                limit=limit,
                fold_groups=False,
            )
            reg_results = golden.apply_golden_boost(conn, q, "register", reg.results)
            # #859: drop boosted pins the steward does not hold (the reg_meta hits
            # are already `fqids`-scoped; the boost prepends pins from a separate
            # source, so re-apply the same filter to them). No-op when `fqids` is
            # None (the `global` deployment).
            reg_results = _scope_to_fqids(reg_results, fqids)
            # `search(type="register")` yields only register rows, but the static
            # element type is the broad `SearchResult` union — narrow to what the
            # group declares (the `type=` param is the runtime guarantee).
            # total_count counts the full boosted set (incl. a net-new pin), but the
            # displayed page is capped at `limit` — a pin prepended onto an already-full
            # FTS page must not push the group past the requested cap (#393 item 2).
            # `reg.total_count` is now query-time-exact (already `fqids`-scoped), so
            # the delta counts only net-new HELD pins.
            groups.append(
                RegisterSearchGroup(
                    total_count=reg.total_count + (len(reg_results) - len(reg.results)),
                    results=cast(
                        "list[RegisterSearchResult]", list(reg_results[:limit])
                    ),
                )
            )
        if want_variable:
            var = reg_meta_search(
                conn,
                q,
                field="description",
                type="variable",
                fqids=fqids,
                limit=limit,
            )
            var_results = golden.apply_golden_boost(conn, q, "variable", var.results)
            # #859: same boost re-filter as the register arm.
            var_results = _scope_to_fqids(var_results, fqids)
            groups.append(
                VariableSearchGroup(
                    total_count=var.total_count + (len(var_results) - len(var.results)),
                    results=cast("list[VariableSearchItem]", list(var_results[:limit])),
                )
            )
        if want_classification:
            cls = reg_meta_search(
                conn,
                q,
                field="description",
                type="classification",
                limit=limit,
            )
            cls_results = golden.apply_golden_boost(
                conn, q, "classification", cls.results
            )
            groups.append(
                ClassificationSearchGroup(
                    total_count=cls.total_count + (len(cls_results) - len(cls.results)),
                    results=cast(
                        "list[ClassificationSearchItem]", list(cls_results[:limit])
                    ),
                )
            )
        if want_value:
            # Codes (#352): the value/code surface — `value_code_fts` label match +
            # code-shape exact/prefix match, NOT the FTS description path. reg_meta
            # ranks (bm25 + rarity downweight) and annotates each hit with its
            # owning variables/classifications. Codes don't fold into concept
            # groups. Golden-boost runs first (a no-op here — no `value` pins are
            # supported, see reg_webapp.golden), THEN `_rank_codes` re-sorts the
            # page so classification-backed codes lead (#393 item 2).
            codes = reg_meta_search(
                conn,
                q,
                field="value",
                type="value",
                limit=limit,
                fold_groups=False,
            )
            boosted_codes = cast(
                "list[CodeSearchResult]",
                golden.apply_golden_boost(conn, q, "value", codes.results),
            )
            # Rank the FULL boosted set first (so a high-ranking net-new pin can lead),
            # THEN cap the displayed page at `limit`; total_count still counts the full
            # boosted set (incl. net-new) so the cap doesn't hide the true match volume.
            code_results = _rank_codes(boosted_codes)[:limit]
            groups.append(
                CodeSearchGroup(
                    total_count=codes.total_count
                    + (len(boosted_codes) - len(codes.results)),
                    results=code_results,
                )
            )

    return SearchResponse(query=q, groups=groups)
