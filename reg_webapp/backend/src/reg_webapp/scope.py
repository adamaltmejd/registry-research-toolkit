"""Project operations always use artifact identity, never a browse scope."""

from __future__ import annotations

from fastapi import HTTPException, Request


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
