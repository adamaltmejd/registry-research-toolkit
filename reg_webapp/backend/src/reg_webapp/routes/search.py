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
from reg_meta.queries import search as reg_meta_search

from reg_webapp.conn import catalog_conn
from reg_webapp.models import (
    ClassificationSearchGroup,
    ClassificationSearchResult,
    ConceptGroupMemberModel,
    ConceptGroupSearchResult,
    GroupFacetModel,
    RegisterSearchGroup,
    RegisterSearchResult,
    SearchResponse,
    VariableSearchGroup,
    VariableSearchResult,
)

router = APIRouter(prefix="/api")

# Input bounds. The query is a bound FTS parameter (no SQLi surface), so these
# guard cost/abuse and the NUL byte sqlite rejects — not injection.
_MAX_QUERY_LEN = 200
_DEFAULT_LIMIT = 20
_MAX_LIMIT = 50

# A "real" token carries at least one unicode alphanumeric char; pure
# punctuation tokenizes to nothing in FTS5 and would yield an empty phrase.
_WORD_CHAR = re.compile(r"[^\W_]", re.UNICODE)


def _validated_query(q: str) -> str:
    """``?q`` gate: reject an over-long query or one carrying a NUL byte (sqlite
    raises on embedded NUL) with 422. A blank / whitespace / punctuation-only
    query is NOT an error — it yields empty groups (see ``_has_searchable_token``)."""
    if "\x00" in q:
        raise HTTPException(status_code=422, detail="query may not contain NUL")
    if len(q) > _MAX_QUERY_LEN:
        raise HTTPException(
            status_code=422,
            detail=f"query too long (max {_MAX_QUERY_LEN} characters)",
        )
    return q


def _validated_limit(limit: int = _DEFAULT_LIMIT) -> int:
    """``?limit`` gate: per-group display cap. Clamped to [1, _MAX_LIMIT] rather
    than 422'd — a researcher nudging the number shouldn't hit an error wall."""
    return max(1, min(limit, _MAX_LIMIT))


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


def _apply_golden_boost(results: list[dict]) -> list[dict]:
    """No-op seam for the curated golden/starred boost (#311). reg_meta already
    sorted ``results`` by FTS rank; once #311 ships a golden list, this is where
    starred hits get promoted within their group. Identity until then — kept as a
    named hook so the call sites and ordering contract are already in place."""
    return results


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
    q: str = Depends(_validated_query),
    limit: int = Depends(_validated_limit),
) -> SearchResponse:
    """Search registers, variables (concept-folded, #322), and classifications
    over the shipped FTS indexes. Each group is an independent reg_meta
    `search(field="description")` call (one per type) so each carries its own
    `total_count` and per-group `limit`. A query with no usable token returns the
    three groups empty (total 0) — not a 422."""
    if not _has_searchable_token(q):
        return SearchResponse(
            query=q,
            groups=[
                RegisterSearchGroup(total_count=0, results=[]),
                VariableSearchGroup(total_count=0, results=[]),
                ClassificationSearchGroup(total_count=0, results=[]),
            ],
        )

    with catalog_conn(request) as conn:
        # `field="description"` is reg_meta's FTS path (register_fts +
        # variable_fts + classification_fts) — NOT the LIKE-based
        # datacolumn/varname/value fields (codes are #352's own group). reg_meta
        # builds the safe FTS MATCH expression from the raw query internally; one
        # call per type yields a per-group total_count + limit. Registers have no
        # concept groups, so folding is off there.
        reg = reg_meta_search(
            conn,
            q,
            field="description",
            type="register",
            limit=limit,
            fold_groups=False,
        )
        var = reg_meta_search(
            conn,
            q,
            field="description",
            type="variable",
            limit=limit,
        )
        cls = reg_meta_search(
            conn,
            q,
            field="description",
            type="classification",
            limit=limit,
        )

    reg_results = _apply_golden_boost(reg["results"])
    var_results = _apply_golden_boost(var["results"])
    cls_results = _apply_golden_boost(cls["results"])

    return SearchResponse(
        query=q,
        groups=[
            RegisterSearchGroup(
                total_count=reg["total_count"],
                results=[_register_result(r) for r in reg_results],
            ),
            VariableSearchGroup(
                total_count=var["total_count"],
                results=[
                    _group_result(r) if r["type"] == "group" else _variable_result(r)
                    for r in var_results
                ],
            ),
            ClassificationSearchGroup(
                total_count=cls["total_count"],
                results=[
                    _group_result(r)
                    if r["type"] == "group"
                    else _classification_result(r)
                    for r in cls_results
                ],
            ),
        ],
    )
