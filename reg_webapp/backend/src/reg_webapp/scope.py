"""HTTP browse scope; project operations always use artifact identity."""

from __future__ import annotations

from fastapi import HTTPException, Request
from reg_meta.holdings import ReadScope  # noqa: TC002 - FastAPI resolves annotations


def browse_scope(request: Request, scope: ReadScope | None = None) -> ReadScope:
    """Validate before opening a connection and expose the effective read scope."""
    kind = request.app.state.manifest["catalog_artifact_kind"]
    if scope == "holdings" and kind != "steward":
        raise HTTPException(
            status_code=422,
            detail=[
                {
                    "loc": ["query", "scope"],
                    "type": "scope_unavailable",
                    "msg": "Holdings scope requires a steward artifact.",
                }
            ],
        )
    effective = scope or ("holdings" if kind == "steward" else "reference")
    request.state.read_scope = effective
    return effective


def reject_project_scope(request: Request) -> None:
    """A browse override cannot change validation or physical ordering."""
    if "scope" in request.query_params:
        raise HTTPException(
            status_code=422,
            detail=[
                {
                    "loc": ["query", "scope"],
                    "type": "scope_forbidden",
                    "msg": "Project endpoints do not accept browse scope.",
                }
            ],
        )
