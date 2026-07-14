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
import unicodedata
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from reg_meta.errors import RegMetaError
from reg_meta.queries import SEARCH_TYPES, search as reg_meta_search

from reg_webapp import golden
from reg_webapp.conn import catalog_conn
from reg_webapp.models import (
    ClassificationCodeSearchGroup,
    ClassificationSearchGroup,
    RegisterSearchGroup,
    RegisterValueSetSearchGroup,
    SearchGroup,
    SearchResponse,
    TopSearchGroup,
    VariableSearchGroup,
)
from reg_webapp.query_input import validate_text_query

if TYPE_CHECKING:
    from reg_meta.search import (
        CodeSearchResult,
        RegisterSearchResult,
        SearchResult,
        SearchResults,
    )

    from reg_webapp.catalog_index import CatalogIndex
    from reg_webapp.models import (
        ClassificationSearchItem,
        TopSearchItem,
        VariableSearchItem,
    )

router = APIRouter(prefix="/api")

_DEFAULT_LIMIT = 3
_MAX_LIMIT = 50
_TOP_RESULTS_LIMIT = 5
_GROUP_LABEL_MATCH_BONUS = 50
_GROUP_MATCHED_MEMBER_BONUS_CAP = 50
_WEB_SEARCH_TYPES = SEARCH_TYPES | {"classification_code", "register_value"}

# A "real" token carries at least one unicode alphanumeric char; pure
# punctuation tokenizes to nothing in FTS5 and would yield an empty phrase.
_WORD_CHAR = re.compile(r"[^\W_]", re.UNICODE)


def _search_boundary(*args: Any, **kwargs: Any) -> SearchResults:
    """Translate reg_meta's usage boundary into this HTTP boundary."""
    try:
        return reg_meta_search(*args, **kwargs)
    except RegMetaError as exc:
        raise HTTPException(status_code=422, detail=exc.message) from exc


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
    if type not in _WEB_SEARCH_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"invalid type '{type}' (valid: {sorted(_WEB_SEARCH_TYPES)})",
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


def _fold_match_text(value: object) -> str:
    text = str(value).strip().casefold()
    decomposed = unicodedata.normalize("NFKD", text)
    folded = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", folded)


def _fqid_leaf(value: object | None) -> str | None:
    if value is None:
        return None
    parts = str(value).split("/")
    return parts[-1] if parts and parts[-1] else None


def _fqid_scope(value: object | None) -> str | None:
    if value is None:
        return None
    parts = str(value).split("/")
    return "/".join(parts[:2]) if len(parts) >= 2 and all(parts[:2]) else None


def _candidate_identity_texts(result: SearchResult) -> tuple[str, ...]:
    texts: list[object] = []
    if result.type == "register":
        texts.extend((result.fqid, _fqid_leaf(result.fqid), result.name))
    elif result.type == "variable":
        texts.extend((result.fqid, _fqid_leaf(result.fqid), result.name))
        texts.extend(result.delivery_column_names)
    elif result.type == "classification":
        texts.extend(
            (
                result.fqid,
                _fqid_leaf(result.fqid),
                result.short_name,
                result.name,
                result.terminal_fqid,
                _fqid_leaf(result.terminal_fqid),
            )
        )
    elif result.type == "classification_succession":
        texts.extend(
            (result.fqid, _fqid_leaf(result.fqid), result.short_name, result.name)
        )
        for edition in result.editions:
            texts.extend(
                (edition.fqid, _fqid_leaf(edition.fqid), edition.slug, edition.name)
            )
    elif result.type == "group":
        texts.extend(
            (result.group_key, _fqid_leaf(result.group_key), result.group_label)
        )
        for member in result.members:
            texts.extend(
                (
                    member.fqid,
                    _fqid_leaf(member.fqid),
                    member.name,
                    member.delivery_column,
                )
            )
            for facet in member.facets:
                texts.extend((facet.value, facet.label))
    elif result.type == "code":
        texts.extend((result.code, result.label, result.code_system))
    return tuple(_fold_match_text(text) for text in texts if text is not None)


def _type_prior(result: SearchResult) -> int:
    if result.type == "register":
        return 40
    if result.type == "variable":
        return 30
    if result.type == "group":
        return 30 if result.kind == "variable" else 25
    if result.type in ("classification", "classification_succession"):
        return 25
    if result.type == "code":
        return 10
    return 0


def _group_authority_bonus(result: SearchResult) -> int:
    if result.type != "group" or not result.label_matched:
        return 0
    matched = max(0, result.matched_count)
    return _GROUP_LABEL_MATCH_BONUS + min(matched, _GROUP_MATCHED_MEMBER_BONUS_CAP)


def _best_bet_score(query: str, result: SearchResult) -> int:
    folded_query = _fold_match_text(query)
    identity_texts = _candidate_identity_texts(result)
    if not folded_query or not identity_texts:
        return _type_prior(result)
    exact = any(text == folded_query for text in identity_texts)
    prefix = any(text.startswith(folded_query) for text in identity_texts)
    return (
        _type_prior(result)
        + (1000 if exact else 0)
        + (100 if prefix and not exact else 0)
        + _group_authority_bonus(result)
    )


def _looks_like_golden_pin(result: SearchResult) -> bool:
    """Golden pins are order-prepended with rank=0.0; keep them pinned when the
    display pass applies best-bet scoring inside typed groups."""
    return result.type in ("register", "classification") and result.rank == 0.0


def _rank_display_results(
    query: str, results: list[SearchResult]
) -> list[SearchResult]:
    """Apply the same query-sensitive best-bet score within one typed section.

    Top results uses `_best_bet_score` to merge across typed groups. Reusing that
    score inside each category keeps an exact/prefix hit from leading Top results
    while sitting lower in its own category. Golden pins stay first because their
    contract is stronger than FTS/order scoring.
    """
    return [
        result
        for _, result in sorted(
            enumerate(results),
            key=lambda item: (
                _looks_like_golden_pin(item[1]),
                _best_bet_score(query, item[1]),
                -item[0],
            ),
            reverse=True,
        )
    ]


def _boosted_continuation(
    origin: SearchResults,
    boosted: list[SearchResult],
    *,
    limit: int,
) -> tuple[bool, str | None]:
    """Advance only past origin rows that survive a pin-prepended page.

    A net-new golden pin can displace an origin hit. Returning the origin page's
    ordinary ``next_cursor`` would skip that hit forever. reg_meta exposes
    excluded-from-wire cursors at each bounded prefix so this presentation layer
    can continue after exactly the origin rows it consumed.
    """
    displaced = len(boosted) > limit
    if not displaced:
        return origin.has_more, origin.next_cursor
    net_new = sum(not any(item == row for row in origin.results) for item in boosted)
    consumed = min(len(origin.results), max(0, limit - net_new))
    next_cursor = (
        origin.page_cursor if consumed == 0 else origin.cursors_after[consumed - 1]
    )
    return True, next_cursor


def _top_candidate_key(result: SearchResult, group_order: int, row_order: int) -> str:
    if result.type == "group":
        if result.kind == "classification":
            return f"group:{result.kind}:{result.group_key}"
        scopes = sorted(
            scope
            for scope in {_fqid_scope(member.fqid) for member in result.members}
            if scope is not None
        )
        scope_key = (
            ",".join(scopes) if scopes else f"unresolved:{group_order}:{row_order}"
        )
        return f"group:{result.kind}:{scope_key}:{result.group_key}"
    if result.type == "code":
        return f"code:{result.code}:{result.label}:{result.code_system}"
    fqid = getattr(result, "fqid", None)
    if fqid is not None:
        return f"{result.type}:{fqid}"
    return f"{result.type}:unresolved:{group_order}:{row_order}"


@dataclass(frozen=True)
class _TopCandidate:
    golden: bool
    score: int
    group_order: int
    row_order: int
    result: SearchResult


def _grouped_variable_member_fqids(candidates: list[_TopCandidate]) -> set[str]:
    fqids: set[str] = set()
    for candidate in candidates:
        result = candidate.result
        if result.type == "group" and result.kind == "variable":
            for member in result.members:
                fqids.add(str(member.fqid))
    return fqids


def _best_bets(
    query: str, groups: list[SearchGroup], *, limit: int
) -> list[TopSearchItem]:
    """Build the cross-group top-results page (#393 items 6/7) from the already
    prepared typed groups.

    This deliberately reuses the same typed rows the normal groups render. The
    score is only a presentation ordering layer: exact identifier/name/code hits
    outrank type priors, and type priors favor register → variable → classification
    → code for broad topical terms. Ties preserve the stable group/result order.
    """
    by_key: dict[str, _TopCandidate] = {}
    for group_order, group in enumerate(groups):
        for row_order, result in enumerate(group.results):
            score = _best_bet_score(query, cast("SearchResult", result))
            candidate = _TopCandidate(
                golden=_looks_like_golden_pin(cast("SearchResult", result)),
                score=score,
                group_order=group_order,
                row_order=row_order,
                result=cast("SearchResult", result),
            )
            key = _top_candidate_key(candidate.result, group_order, row_order)
            current = by_key.get(key)
            if current is None or (
                candidate.golden,
                candidate.score,
                -candidate.group_order,
                -candidate.row_order,
            ) > (
                current.golden,
                current.score,
                -current.group_order,
                -current.row_order,
            ):
                by_key[key] = candidate

    ranked = sorted(
        by_key.values(),
        key=lambda candidate: (
            not candidate.golden,
            -candidate.score,
            candidate.group_order,
            candidate.row_order,
        ),
    )
    grouped_members = _grouped_variable_member_fqids(ranked)
    visible_ranked = [
        candidate
        for candidate in ranked
        if not (
            candidate.result.type == "variable"
            and candidate.result.fqid is not None
            and str(candidate.result.fqid) in grouped_members
        )
    ]
    return [
        cast("TopSearchItem", candidate.result) for candidate in visible_ranked[:limit]
    ]


def _scope_to_fqids(
    results: list[SearchResult], fqids: frozenset[str] | None
) -> list[SearchResult]:
    """Re-filter the golden-boost set to drop unheld register/VARIABLE LEAF pins
    (#859). A no-op when `fqids` is None (the `global` deployment — byte-identical to
    pre-#859). The reg_meta hits are ALREADY `fqids`-scoped query-time; this re-filter
    exists ONLY to drop unheld LEAF pins the golden boost prepended from a separate
    source.

    A row is KEPT when it has NO `fqid` attribute (or `fqid is None`) OR its serialized
    `fqid` is in the held set. The fqid-less pass-through is load-bearing: the variable
    arm can carry `ConceptGroupSearchResult` rows (folded groups / label matches) which
    have no `fqid` — those are already query-time scoped by reg_meta (`_group_result_row`'s
    `allow` narrows members to held), so they must NOT be dropped here, or a filtered
    steward stops seeing held concept groups. Compares the
    SERIALIZED fqid string (the model's `fqid` is an `Fqid | None`; the held set holds
    canonical strings), mirroring `golden.apply_golden_boost`'s dedup."""
    if fqids is None:
        return results
    return [
        r for r in results if (f := getattr(r, "fqid", None)) is None or str(f) in fqids
    ]


def _narrow_search_groups(
    results: list[SearchResult], index: CatalogIndex
) -> tuple[list[SearchResult], int]:
    """Narrow each `ConceptGroupSearchResult` row's `members` to the steward's
    COLUMN-grain holdings (#859), mirroring browse's `_narrow_group_members`.

    reg_meta already narrowed group members at FQID grain (`_group_result_row`'s
    `allow` set), but a #819 representation member shares one FQID across different
    `delivery_column`s — a steward holding only SOME columns of an FQID still sees the
    unheld representations. This refines on top: a representation member
    (`delivery_column` set) is kept iff `index.admits(str(m.fqid), m.delivery_column)`;
    a whole-variable member (`delivery_column` None) iff its bare FQID is in
    `admitted_variable_fqids`. `member_count` is reset to the narrowed length.

    A group left with NO surviving member is DROPPED. The steward variable arm uses
    bounded cursor backfill so a dropped row does not unnecessarily shorten the page.

    Browse's `_narrow_group_members` operates on a DIFFERENT model
    (`ConceptGroupSummary` vs `ConceptGroupSearchResult`), so a thin search-local helper
    is the right reuse boundary — not a forced shared abstraction."""
    admitted = index.admitted_variable_fqids
    kept_rows: list[SearchResult] = []
    dropped = 0
    for r in results:
        # The `.type` discriminator narrows `r` to `ConceptGroupSearchResult` here.
        if r.type != "group":
            kept_rows.append(r)
            continue
        kept_members = [
            m
            for m in r.members
            if (
                index.admits(str(m.fqid), m.delivery_column)
                if m.delivery_column is not None
                else str(m.fqid) in admitted
            )
        ]
        if not kept_members:
            dropped += 1
            continue
        kept_rows.append(
            r.model_copy(
                update={
                    "members": tuple(kept_members),
                    "member_count": len(kept_members),
                }
            )
        )
    return kept_rows, dropped


def _narrow_variable_leaf_columns(
    results: list[SearchResult], index: CatalogIndex
) -> list[SearchResult]:
    """Mask variable leaf delivery-column chips to the steward's held columns."""
    narrowed: list[SearchResult] = []
    for result in results:
        if result.type != "variable" or result.fqid is None:
            narrowed.append(result)
            continue
        held = index.held_columns(str(result.fqid))
        if not held or not result.delivery_column_names:
            narrowed.append(result)
            continue
        held_names = frozenset(col for col in held if col is not None)
        held_columns = tuple(
            col for col in result.delivery_column_names if col in held_names
        )
        narrowed.append(
            result.model_copy(update={"delivery_column_names": held_columns})
        )
    return narrowed


@router.get("/search", response_model=SearchResponse)
def get_search(
    request: Request,
    response: Response,
    q: str = Depends(validate_text_query),
    limit: int = Depends(_validated_limit),
    req_type: str = Depends(_validated_type),
    cursor: str | None = None,
) -> SearchResponse:
    """Search registers, variables (concept-folded, #322), classifications, and
    codes/values (#352) over the shipped FTS indexes. Each typed group is an
    independent reg_meta `search()` call (register/variable/classification via the
    FTS `field="description"` path; codes via the `field="value"` path) so each
    carries its own bounded continuation cursor; the value surface is split
    into classification codes and register-local value sets so each gets its own
    page. The all-scope response prepends a `top_results` best-bets group built
    from those same typed rows when multiple candidates compete (#393 items 6/7);
    scoped responses emit only the requested typed surface (`type=value` emits the
    two value groups). A query with no usable token returns the selected group(s)
    empty (total 0) — not a 422.

    ``?type=`` (#393 item 1) scopes the search: ``all`` (the default) preserves the
    optional top-results→register→variable→classification→value behavior; any
    single type runs AND emits only that typed surface. Group ORDER is fixed for
    the ``all`` case.

    A FILTERED steward (``app.state.catalog_index`` present, #859) scopes the
    REGISTER and VARIABLE surfaces to the steward's held FQIDs — both the reg_meta
    query (the ``fqids`` allow-list, applied query-time before paging)
    and the golden boost (a boosted pin the steward does not hold is dropped). The
    CLASSIFICATION and VALUE/code surfaces are catalog-global and pass through
    unscoped. A global deployment uses the same cursor contract without the steward
    restriction."""
    # Per-type gates: each arm runs (and its group is emitted) only when the
    # requested type selects it. `all` selects every arm.
    want_register = req_type in ("all", "register")
    want_variable = req_type in ("all", "variable")
    want_classification = req_type in ("all", "classification")
    want_classification_code = req_type in ("all", "value", "classification_code")
    want_register_value = req_type in ("all", "value", "register_value")

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

    # Typed groups are appended in the fixed register→variable→classification→value
    # order. The all-scope top-results group is prepended after these typed groups
    # are prepared; `type=value` emits the two value groups.
    groups: list[SearchGroup] = []

    # The empty-query / no-usable-token short-circuit: same group selection as the
    # live path, just empty.
    if not _has_searchable_token(q):
        if want_register:
            groups.append(RegisterSearchGroup(results=[], has_more=False))
        if want_variable:
            groups.append(VariableSearchGroup(results=[], has_more=False))
        if want_classification:
            groups.append(ClassificationSearchGroup(results=[], has_more=False))
        if want_classification_code:
            groups.append(ClassificationCodeSearchGroup(results=[], has_more=False))
        if want_register_value:
            groups.append(RegisterValueSetSearchGroup(results=[], has_more=False))
        return SearchResponse(query=q, groups=groups)

    phase_timings: list[tuple[str, float]] = []
    with catalog_conn(request) as conn:
        # `field="description"` is reg_meta's FTS path (register_fts +
        # variable_fts + classification_fts) — NOT the LIKE-based
        # datacolumn/varname/value fields (codes are #352's own group). reg_meta
        # builds the safe FTS MATCH expression from the raw query internally; one
        # call per type yields an independent bounded page. Registers have no
        # concept groups, so folding is off there.
        # reg_meta `search()` now returns typed `SearchResults` (#701): each arm's
        # rows are ALREADY the right reg_meta result models, so the webapp slices
        # and groups them directly — no per-row re-wrapping. The golden seam and the
        # FastAPI response models all operate on the same reg_meta types.
        if want_register:
            phase_start = perf_counter()
            reg = _search_boundary(
                conn,
                q,
                field="description",
                type="register",
                fqids=fqids,
                limit=limit,
                cursor=cursor,
                fold_groups=False,
            )
            reg_results = (
                golden.apply_golden_boost(conn, q, "register", reg.results)
                if cursor is None
                else list(reg.results)
            )
            # #859: drop boosted pins the steward does not hold (the reg_meta hits
            # are already `fqids`-scoped; the boost prepends pins from a separate
            # source, so re-apply the same filter to them). No-op when `fqids` is
            # None (the `global` deployment).
            reg_results = _scope_to_fqids(reg_results, fqids)
            reg_has_more, reg_next_cursor = _boosted_continuation(
                reg, reg_results, limit=limit
            )
            # `search(type="register")` yields only register rows, but the static
            # element type is the broad `SearchResult` union — narrow to what the
            # group declares (the `type=` param is the runtime guarantee).
            # The displayed page is capped at `limit`. If a net-new pin displaces
            # an origin row, `_boosted_continuation` resumes before that row.
            groups.append(
                RegisterSearchGroup(
                    results=cast(
                        "list[RegisterSearchResult]",
                        list(_rank_display_results(q, reg_results)[:limit]),
                    ),
                    has_more=reg_has_more,
                    next_cursor=reg_next_cursor,
                )
            )
            phase_timings.append(("register", perf_counter() - phase_start))
        if want_variable:
            phase_start = perf_counter()
            delivery_column_scope = (
                {
                    fqid: index.held_columns(fqid)
                    for fqid in index.admitted_variable_fqids
                }
                if index is not None
                else None
            )
            var = _search_boundary(
                conn,
                q,
                field="description",
                type="variable",
                fqids=fqids,
                delivery_column_scope=delivery_column_scope,
                limit=limit,
                cursor=cursor,
            )
            var_results = (
                golden.apply_golden_boost(conn, q, "variable", var.results)
                if cursor is None
                else list(var.results)
            )
            # #859: same boost re-filter as the register arm (drops unheld LEAF pins;
            # group/fqid-less rows pass through).
            var_results = _scope_to_fqids(var_results, fqids)
            var_origin_has_more, var_origin_next_cursor = _boosted_continuation(
                var, var_results, limit=limit
            )
            # #865: reg_meta narrowed group members at FQID grain, but #819
            # representation members share one FQID across `delivery_column`s — a steward
            # holding only some columns still sees the unheld representations. Refine each
            # group row's `members` at COLUMN grain (browse's `_narrow_group_members`
            # equivalent for the search model), dropping a group with no held member and
            # removing it from the bounded page.
            if index is not None:
                var_results = _narrow_variable_leaf_columns(var_results, index)
                var_results, _ = _narrow_search_groups(var_results, index)
                continuation_cursor = var_origin_next_cursor
                continuation_has_more = var_origin_has_more
                # Column-grain steward narrowing can drop a whole folded row.
                # Backfill one bounded candidate at a time so we never skip an
                # unshown origin row when advancing the opaque cursor.
                backfill_budget = limit * 4 + 4
                while (
                    len(var_results) < limit
                    and continuation_has_more
                    and continuation_cursor is not None
                    and backfill_budget > 0
                ):
                    page = _search_boundary(
                        conn,
                        q,
                        field="description",
                        type="variable",
                        fqids=fqids,
                        delivery_column_scope=delivery_column_scope,
                        limit=1,
                        cursor=continuation_cursor,
                    )
                    page_results = _scope_to_fqids(list(page.results), fqids)
                    page_results = _narrow_variable_leaf_columns(page_results, index)
                    page_results, _ = _narrow_search_groups(page_results, index)
                    var_results.extend(page_results)
                    continuation_cursor = page.next_cursor
                    continuation_has_more = page.has_more
                    backfill_budget -= 1
                var_has_more = continuation_has_more
                var_next_cursor = continuation_cursor
            else:
                var_has_more = var_origin_has_more
                var_next_cursor = var_origin_next_cursor
            groups.append(
                VariableSearchGroup(
                    results=cast(
                        "list[VariableSearchItem]",
                        list(_rank_display_results(q, var_results)[:limit]),
                    ),
                    has_more=var_has_more or len(var_results) > limit,
                    next_cursor=var_next_cursor,
                )
            )
            phase_timings.append(("variable", perf_counter() - phase_start))
        if want_classification:
            phase_start = perf_counter()
            cls = _search_boundary(
                conn,
                q,
                field="description",
                type="classification",
                limit=limit,
                cursor=cursor,
            )
            cls_results = (
                golden.apply_golden_boost(conn, q, "classification", cls.results)
                if cursor is None
                else list(cls.results)
            )
            cls_has_more, cls_next_cursor = _boosted_continuation(
                cls, cls_results, limit=limit
            )
            groups.append(
                ClassificationSearchGroup(
                    results=cast(
                        "list[ClassificationSearchItem]",
                        list(_rank_display_results(q, cls_results)[:limit]),
                    ),
                    has_more=cls_has_more,
                    next_cursor=cls_next_cursor,
                )
            )
            phase_timings.append(("classification", perf_counter() - phase_start))
        if want_classification_code:
            phase_start = perf_counter()
            # Codes (#352): the value/code surface — `value_code_fts` label match +
            # code-shape exact/prefix match, NOT the FTS description path. reg_meta
            # ranks (bm25 + rarity downweight) and annotates each hit with its
            # owning variables/classifications. Codes don't fold into concept
            # groups. Split classification-owned codes from register-local value
            # sets before pagination so neither bucket starves the other.
            classification_codes = _search_boundary(
                conn,
                q,
                field="value",
                type="value",
                limit=limit,
                fold_groups=False,
                code_owner_scope="classification",
                cursor=cursor,
            )
            boosted_classification_codes = cast(
                "list[CodeSearchResult]",
                golden.apply_golden_boost(
                    conn, q, "value", classification_codes.results
                )
                if cursor is None
                else list(classification_codes.results),
            )
            code_has_more, code_next_cursor = _boosted_continuation(
                classification_codes,
                cast("list[SearchResult]", boosted_classification_codes),
                limit=limit,
            )
            classification_code_results = _rank_codes(boosted_classification_codes)[
                :limit
            ]
            groups.append(
                ClassificationCodeSearchGroup(
                    results=classification_code_results,
                    has_more=code_has_more,
                    next_cursor=code_next_cursor,
                )
            )
            phase_timings.append(("classification_code", perf_counter() - phase_start))
        if want_register_value:
            phase_start = perf_counter()
            register_values = _search_boundary(
                conn,
                q,
                field="value",
                type="value",
                limit=limit,
                fold_groups=False,
                code_owner_scope="register_local",
                cursor=cursor,
            )
            boosted_register_values = cast(
                "list[CodeSearchResult]",
                golden.apply_golden_boost(conn, q, "value", register_values.results)
                if cursor is None
                else list(register_values.results),
            )
            value_has_more, value_next_cursor = _boosted_continuation(
                register_values,
                cast("list[SearchResult]", boosted_register_values),
                limit=limit,
            )
            register_value_results = _rank_codes(boosted_register_values)[:limit]
            groups.append(
                RegisterValueSetSearchGroup(
                    results=register_value_results,
                    has_more=value_has_more,
                    next_cursor=value_next_cursor,
                )
            )
            phase_timings.append(("register_value", perf_counter() - phase_start))

    if req_type == "all":
        top_total = sum(len(group.results) for group in groups)
        if top_total > 1:
            top_limit = min(limit, _TOP_RESULTS_LIMIT)
            best_bets = _best_bets(q, groups, limit=top_limit)
            groups.insert(
                0,
                TopSearchGroup(
                    results=best_bets,
                ),
            )

    response.headers["Server-Timing"] = ", ".join(
        f"{name};dur={seconds * 1000:.1f}" for name, seconds in phase_timings
    )

    return SearchResponse(query=q, groups=groups)
