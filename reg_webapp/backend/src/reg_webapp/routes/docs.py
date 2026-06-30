"""`GET /api/docs/*` — the documentation library (#354).

Read surface over the prebuilt `reg_meta_docs.db` FTS index (already baked into
the deployed container, previously unopened by the webapp). Reuses reg_meta's
read-only query layer (`doc_search` / `doc_get` / `doc_registers`) — no new query
logic here beyond plumbing + the response policy.

POLICY for the FTS documentation library: serve EXCERPTS + a pointer to the SCB
source, NEVER the full converted body (marker+Gemini conversion quality +
republication exposure). The related-document PDF surface (#742) is separate:
those binaries are curated/rehosted under the #739 CC BY 4.0 license basis and
served verbatim with attribution metadata.

The docs DB is OPTIONAL: when absent / schema-incompat, `app.state.docs_db_path`
is None (set at boot) and every endpoint degrades to an `ingested=False` / 404
"not ingested" response rather than 500 — a missing docs index must not break the
catalog API. Connection model = the shared per-request open (`conn.docs_conn`),
same threadpool-safe pattern as the catalog routes. GET reads, so the ETag
middleware covers caching (query is in the URL → edge cache key + in the body →
ETag).
"""

from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response
from reg_meta.doc_queries import (
    doc_get,
    doc_registers,
    doc_search,
    related_document_content,
    related_documents_for_register,
)
from reg_meta.fqid import FqidError, validate_slug

from reg_webapp.conn import docs_conn
from reg_webapp.models import (
    DocDetail,
    DocResult,
    DocSearchResponse,
    DocVariableMentions,
    RelatedDocument,
    RelatedDocumentsResponse,
)
from reg_webapp.query_input import validate_text_query

if TYPE_CHECKING:
    from reg_meta.doc_db import RelatedDocument as RegMetaRelatedDocument

router = APIRouter(prefix="/api/docs")

_DEFAULT_LIMIT = 20
_MAX_LIMIT = 50
# Detail view serves a bounded preview, never the full body (republication).
_EXCERPT_CHARS = 500


def _validated_limit(limit: int = _DEFAULT_LIMIT) -> int:
    """``?limit`` display cap, clamped to [1, _MAX_LIMIT] (not 422'd)."""
    return max(1, min(limit, _MAX_LIMIT))


def _validated_register(register: str | None = None) -> str | None:
    """Optional ``?register`` scope. Bound parameter (no SQLi), so this only
    guards the NUL byte sqlite rejects; the value is matched verbatim against
    `doc.register` (a slug), so a non-matching value simply yields no rows."""
    if register is not None and "\x00" in register:
        raise HTTPException(status_code=422, detail="register may not contain NUL")
    return register


def _validated_register_slug(register: str) -> str:
    """Register slug path gate for related-document endpoints."""
    try:
        validate_slug(register, "register")
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return register


def _validated_filename(filename: str) -> str:
    """Register-local PDF filename path gate.

    The DB query is parameterized, but the filename also feeds
    Content-Disposition, so reject path separators and control characters at the
    HTTP boundary.
    """
    if (
        not filename
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
        or any(ord(ch) < 32 or ord(ch) == 127 for ch in filename)
    ):
        raise HTTPException(status_code=422, detail="invalid related-document filename")
    return filename


def _docs_available(request: Request) -> bool:
    return request.app.state.docs_db_path is not None


def _doc_result(r: dict, *, fuzzy: bool = False) -> DocResult:
    return DocResult(
        register=r.get("register"),
        variable=r.get("variable"),
        filename=r["filename"],
        display_name=r.get("display_name"),
        tags=r.get("tags", []),
        snippet=r.get("snippet"),
        source=r.get("source"),
        # Resolved at doc-DB build from the curated source→PDF map (#372); None
        # when the source is uncurated.
        source_url=r.get("source_url"),
        source_title=r.get("source_title"),
        fuzzy=fuzzy,
    )


def _related_document(doc: RegMetaRelatedDocument) -> RelatedDocument:
    return RelatedDocument(**doc.model_dump())


def _excerpt(body_clean: str | None) -> str | None:
    """A bounded preview of the cleaned body — never the full text."""
    if not body_clean:
        return None
    text = body_clean.strip()
    if not text:
        return None
    if len(text) <= _EXCERPT_CHARS:
        return text
    return text[:_EXCERPT_CHARS].rstrip() + "…"


def _content_disposition(filename: str) -> str:
    fallback = "".join(
        ch if 32 <= ord(ch) < 127 and ch not in {'"', "\\"} else "_" for ch in filename
    )
    fallback = fallback or "document.pdf"
    encoded = urllib.parse.quote(filename, safe="")
    return f"inline; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"


@router.get("/related/{register}", response_model=RelatedDocumentsResponse)
def get_related_documents(request: Request, register: str) -> RelatedDocumentsResponse:
    """List rehosted register-version PDFs for one register.

    Missing docs DB degrades to `ingested=False` instead of failing the catalog
    page. A present DB with no rows returns an empty list.
    """
    register = _validated_register_slug(register)
    if not _docs_available(request):
        return RelatedDocumentsResponse(
            ingested=False,
            register=register,
            documents=[],
        )
    with docs_conn(request) as conn:
        docs = related_documents_for_register(conn, register)
    return RelatedDocumentsResponse(
        ingested=True,
        register=register,
        documents=[_related_document(doc) for doc in docs],
    )


@router.get(
    "/file/{register}/{filename}",
    response_class=Response,
    responses={200: {"content": {"application/pdf": {}}}},
)
def get_related_document_file(
    request: Request, register: str, filename: str
) -> Response:
    """Serve one rehosted related-document PDF by register-local filename."""
    register = _validated_register_slug(register)
    filename = _validated_filename(filename)
    if not _docs_available(request):
        raise HTTPException(status_code=404, detail="documentation index not ingested")
    with docs_conn(request) as conn:
        doc = related_document_content(conn, register, filename)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"no related document {filename!r} for register {register!r}",
        )
    return Response(
        content=doc.content,
        media_type="application/pdf",
        headers={
            "Content-Disposition": _content_disposition(doc.filename),
            "Content-Length": str(doc.byte_size),
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/search", response_model=DocSearchResponse)
def get_docs_search(
    request: Request,
    q: str = Depends(validate_text_query),
    register: str | None = Depends(_validated_register),
    limit: int = Depends(_validated_limit),
    offset: int = 0,
) -> DocSearchResponse:
    """Full-text docs search, optionally register-scoped. When the docs index is
    absent, `ingested=False` with empty results (NOT a 500) — "no docs ingested",
    distinct from an empty result set for a real query."""
    if not _docs_available(request):
        return DocSearchResponse(query=q, ingested=False, total_count=0, results=[])
    offset = max(0, offset)
    with docs_conn(request) as conn:
        out = doc_search(conn, q, register=register, limit=limit, offset=offset)
    return DocSearchResponse(
        query=q,
        ingested=True,
        total_count=out["total_count"],
        results=[_doc_result(r) for r in out["results"]],
    )


@router.get("/for-variable", response_model=DocVariableMentions)
def get_docs_for_variable(
    request: Request,
    q: str = Depends(validate_text_query),
    register: str | None = Depends(_validated_register),
    limit: int = Depends(_validated_limit),
) -> DocVariableMentions:
    """The "mentioned in documentation" hook for a variable leaf: FUZZY
    name/provider_key text search over the docs index (the SPA passes the
    variable's provider_key and/or name as `q`, scoped to its register).

    `register_ingested` encodes the coverage distinction — when the variable's
    register has no ingested docs (coverage is LISA-only), absence reads as "no
    docs ingested for this register", NOT "this variable is undocumented". Every
    result is flagged `fuzzy` (a heuristic text match, not an authoritative
    variable→doc link)."""
    if not _docs_available(request):
        return DocVariableMentions(
            ingested=False,
            register_ingested=False,
            register=register,
            total_count=0,
            results=[],
        )
    with docs_conn(request) as conn:
        register_ingested = register is not None and register in doc_registers(conn)
        out = doc_search(conn, q, register=register, limit=limit)
    return DocVariableMentions(
        ingested=True,
        register_ingested=register_ingested,
        register=register,
        total_count=out["total_count"],
        results=[_doc_result(r, fuzzy=True) for r in out["results"]],
    )


@router.get("/doc/{identifier}", response_model=DocDetail)
def get_doc(request: Request, identifier: str) -> DocDetail:
    """One doc by variable name or filename — metadata + source pointer + a
    BOUNDED excerpt (never the full body). 404 when the docs index is absent
    (detail says so, distinct from a genuine not-found) or the doc isn't found."""
    if not _docs_available(request):
        raise HTTPException(status_code=404, detail="documentation index not ingested")
    # No NUL guard: the HTTP transport + Starlette reject a NUL in the path
    # before the handler, and `identifier` reaches sqlite only as a bound
    # parameter (no injection surface).
    with docs_conn(request) as conn:
        doc = doc_get(conn, identifier)
    if doc is None:
        raise HTTPException(
            status_code=404, detail=f"no documentation for {identifier!r}"
        )
    return DocDetail(
        register=doc.get("register"),
        variable=doc.get("variable"),
        filename=doc["filename"],
        display_name=doc.get("display_name"),
        tags=doc.get("tags", []),
        source=doc.get("source"),
        source_url=doc.get("source_url"),
        source_title=doc.get("source_title"),
        excerpt=_excerpt(doc.get("body_clean")),
    )
