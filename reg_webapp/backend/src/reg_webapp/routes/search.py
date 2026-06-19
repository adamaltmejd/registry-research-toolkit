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

from fastapi import APIRouter, Depends, HTTPException, Request
from reg_meta.queries import SEARCH_TYPES, search as reg_meta_search

from reg_webapp import golden
from reg_webapp.conn import catalog_conn
from reg_webapp.models import (
    ClassificationEditionModel,
    ClassificationSearchGroup,
    ClassificationSearchItem,
    ClassificationSearchResult,
    ClassificationSuccessionSearchResult,
    CodeOwnerClassification,
    CodeOwnerVariable,
    CodeSearchGroup,
    CodeSearchResult,
    ConceptGroupMemberModel,
    ConceptGroupSearchResult,
    GroupFacetModel,
    RegisterSearchGroup,
    RegisterSearchResult,
    SearchGroup,
    SearchResponse,
    VariableSearchGroup,
    VariableSearchResult,
)
from reg_webapp.query_input import validate_text_query

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


def _rank_codes(results: list[dict]) -> list[dict]:
    """Re-rank a code/value page so classification-backed (curated) codes lead,
    then by classification_count, then variable_count — all DESCENDING; FTS order
    is preserved within ties (stable sort, #393 item 2).

    LIMITATION (deferred annotation): reg_meta returns the FTS-top-N codes and only
    THEN annotates the shown page with owner counts (`_annotate_value_page`), so the
    counts exist only on the rows already in the page. This re-sort therefore only
    reorders WITHIN that page — it cannot pull a curated code that ranked below the
    FTS cutoff into view. Accepted as an easy-win until ranking moves into reg_meta.

    Pure helper (no IO) so it's unit-testable in isolation."""
    return sorted(
        results,
        key=lambda r: (
            r.get("classification_count", 0) > 0,
            r.get("classification_count", 0),
            r.get("variable_count", 0),
        ),
        reverse=True,
    )


def _code_system(classifications: list[dict]) -> str | None:
    """The code system a code belongs to (#393 item 3): the primary/first owning
    classification's `short_name` (fall back to `name`). None for register-local /
    bespoke codes with no owning classification. Pure helper (unit-testable)."""
    if not classifications:
        return None
    first = classifications[0]
    return first.get("short_name") or first.get("name")


def _register_result(r: dict) -> RegisterSearchResult:
    return RegisterSearchResult(
        fqid=r.get("fqid"),
        name=r.get("register_name"),
        purpose=r.get("register_purpose"),
    )


def _variable_result(r: dict) -> VariableSearchResult:
    return VariableSearchResult(
        fqid=r.get("fqid"),
        name=r.get("variable_name"),
        register=r.get("register_name"),
        definition=r.get("variable_definition"),
        concept_group=r.get("concept_group"),
        concept_group_label=r.get("concept_group_label"),
    )


def _classification_result(r: dict) -> ClassificationSearchResult:
    return ClassificationSearchResult(
        fqid=r.get("fqid"),
        short_name=r.get("short_name"),
        name=r.get("classification_name"),
        # Preserve the lone-member family hint (symmetric with variables) so a
        # non-folded vintage member stays discoverable.
        concept_group=r.get("concept_group"),
        concept_group_label=r.get("concept_group_label"),
        # A lone non-terminal edition hit (#571) carries the current edition's
        # fqid so the SPA can offer a "go to current edition" link; absent/None
        # for a current edition or a non-edition classification.
        terminal_fqid=r.get("terminal_fqid"),
    )


def _classification_succession_result(
    r: dict,
) -> ClassificationSuccessionSearchResult:
    """Map a reg_meta `type: "classification_succession"` fold row (#571) onto the
    wire model: the terminal edition's identity (`fqid`/`short_name`/`name` from
    `classification_name`) + the full `editions` chain + `matched_count` (how many
    editions the query hit, from the raw `matched` leaf-hit list)."""
    return ClassificationSuccessionSearchResult(
        fqid=r.get("fqid"),
        short_name=r.get("short_name"),
        name=r.get("classification_name"),
        editions=[
            ClassificationEditionModel(
                slug=e["slug"],
                fqid=e.get("fqid"),
                name=e.get("name"),
                effective_year=e.get("effective_year"),
            )
            for e in r.get("editions", [])
        ],
        matched_count=len(r.get("matched") or []),
    )


def _classification_search_item(r: dict) -> ClassificationSearchItem:
    """Dispatch one classifications-arm row to its wire model: an umbrella
    concept-group fold (`type: "group"`, #516) → `_group_result`; an edition
    succession fold (`type: "classification_succession"`, #571) →
    `_classification_succession_result`; otherwise a leaf classification hit (which
    may carry a `terminal_fqid` for a lone old-edition row — surfaced on
    `ClassificationSearchResult` so the SPA can link to the current edition) →
    `_classification_result`."""
    row_type = r.get("type")
    if row_type == "group":
        return _group_result(r)
    if row_type == "classification_succession":
        return _classification_succession_result(r)
    return _classification_result(r)


def _code_result(r: dict) -> CodeSearchResult:
    # reg_meta's value/code search (type="value") already bounds the owner lists
    # and carries the full counts; the webapp just re-shapes them. `register` is
    # the owning register's display name for each variable owner.
    return CodeSearchResult(
        code=r["code"],
        label=r["label"],
        variables=[
            CodeOwnerVariable(
                fqid=v.get("fqid"), name=v.get("name"), register=v.get("register")
            )
            for v in r.get("variables", [])
        ],
        variable_count=r.get("variable_count", 0),
        classifications=[
            CodeOwnerClassification(
                fqid=c.get("fqid"),
                short_name=c.get("short_name"),
                name=c.get("name"),
            )
            for c in r.get("classifications", [])
        ],
        classification_count=r.get("classification_count", 0),
        code_system=_code_system(r.get("classifications", [])),
    )


def _group_result(r: dict) -> ConceptGroupSearchResult:
    return ConceptGroupSearchResult(
        kind=r["kind"],
        group_key=r["group_key"],
        group_label=r["group_label"],
        source=r.get("group_source"),
        register=r.get("register_name"),
        member_count=r.get("member_count", 0),
        matched_count=len(r.get("matched") or []),
        label_matched=r.get("label_matched", False),
        members=[
            ConceptGroupMemberModel(
                fqid=m["fqid"],
                name=m.get("name"),
                facets=[GroupFacetModel(**f) for f in m.get("facets", [])],
            )
            for m in r.get("members", [])
        ],
    )


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
    AND emits only that one group. Group ORDER is fixed for the ``all`` case."""
    # Per-type gates: each arm runs (and its group is emitted) only when the
    # requested type selects it. `all` selects every arm.
    want_register = req_type in ("all", "register")
    want_variable = req_type in ("all", "variable")
    want_classification = req_type in ("all", "classification")
    want_value = req_type in ("all", "value")

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
        if want_register:
            reg = reg_meta_search(
                conn,
                q,
                field="description",
                type="register",
                limit=limit,
                fold_groups=False,
            )
            reg_results = golden.apply_golden_boost(conn, q, "register", reg["results"])
            # total_count counts the full boosted set (incl. a net-new pin), but the
            # displayed page is capped at `limit` — a pin prepended onto an already-full
            # FTS page must not push the group past the requested cap (#393 item 2).
            groups.append(
                RegisterSearchGroup(
                    total_count=reg["total_count"]
                    + (len(reg_results) - len(reg["results"])),
                    results=[_register_result(r) for r in reg_results[:limit]],
                )
            )
        if want_variable:
            var = reg_meta_search(
                conn,
                q,
                field="description",
                type="variable",
                limit=limit,
            )
            var_results = golden.apply_golden_boost(conn, q, "variable", var["results"])
            groups.append(
                VariableSearchGroup(
                    total_count=var["total_count"]
                    + (len(var_results) - len(var["results"])),
                    results=[
                        _group_result(r)
                        if r["type"] == "group"
                        else _variable_result(r)
                        for r in var_results[:limit]
                    ],
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
                conn, q, "classification", cls["results"]
            )
            groups.append(
                ClassificationSearchGroup(
                    total_count=cls["total_count"]
                    + (len(cls_results) - len(cls["results"])),
                    results=[
                        _classification_search_item(r) for r in cls_results[:limit]
                    ],
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
            boosted_codes = golden.apply_golden_boost(
                conn, q, "value", codes["results"]
            )
            # Rank the FULL boosted set first (so a high-ranking net-new pin can lead),
            # THEN cap the displayed page at `limit`; total_count still counts the full
            # boosted set (incl. net-new) so the cap doesn't hide the true match volume.
            code_results = _rank_codes(boosted_codes)[:limit]
            groups.append(
                CodeSearchGroup(
                    total_count=codes["total_count"]
                    + (len(boosted_codes) - len(codes["results"])),
                    results=[_code_result(r) for r in code_results],
                )
            )

    return SearchResponse(query=q, groups=groups)
