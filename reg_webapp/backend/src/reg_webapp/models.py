"""Webapp-local Pydantic response models.

These are reg_webapp's OWN response models (see DESIGN.md → Pydantic boundary) for
the project-write and docs-library routes. The catalog pages read the Rust server,
so no catalog node models live here. reg_meta's frozen Pydantic models (here
`OrderFinding`) are embedded directly rather than re-modeled.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel
from reg_meta.order import OrderFinding  # noqa: TC002 — a runtime model field

# ── A5.2b-ii write surface (see DESIGN.md → Project-write surface
# (routes/project.py)) ───────────────────────────────────────────────────────
# `POST /api/project/validate` returns the concatenated issue list. The
# webapp wraps reg_schema's FROZEN `ValidationResult` / `ValidationIssue`
# dataclasses (see DESIGN.md → Pydantic boundary: reg_schema stays a dataclass —
# it's consumed by the SPA — so the webapp Pydantic-wraps it 1:1). This is the ONLY place reg_schema's
# ValidationResult is re-modeled; the rest of the write surface (`/order`) takes
# `reg_schema.ProjectData` directly as the typed request body.


class ValidationIssueModel(BaseModel):
    """One validation issue — a 1:1 Pydantic wrapper of reg_schema's frozen
    ``ValidationIssue`` dataclass. ``level`` is the tri-state severity; ``path`` is
    an RFC-6901 JSON pointer into ``project_data.json`` (empty for whole-document
    issues); ``code`` is the stable, namespaced rule identifier the SPA maps to a
    UI affordance."""

    level: Literal["error", "warning", "info"]
    code: str
    path: str
    message: str
    successor_fqid: str | None = None


class ValidationResultModel(BaseModel):
    """`POST /api/project/validate` response — the concatenated issue list
    (structural ⧺ semantic) plus the derived ``ok`` flag. The body is reg_meta's
    ``semantic.validation_json`` verbatim; this model types it for OpenAPI.

    ``ok`` mirrors ``reg_schema.ValidationResult.ok``: True iff NO error-level
    issue is present (warnings/info do not flip it). A validation FAILURE is a
    SUCCESSFUL validation RESPONSE — this carries HTTP 200 with ``ok=false`` and
    the issues; 4xx is reserved for a malformed request (bad JSON / oversized body
    / wrong content-type), never for a spec that simply failed to validate."""

    ok: bool
    issues: list[ValidationIssueModel]


class OrderBlockedModel(BaseModel):
    """`POST /api/project/order` 422 body — the "this is not an order" result.

    Fail-closed is a CONTRACT, not a message: the findings ride as reg_meta's own
    frozen ``OrderFinding`` models (embedded directly, like every other reg_meta
    shape here), each carrying its stable ``code``, its message, and the optional
    ``source`` / ``variable`` / ``period`` coordinates that say WHERE — so the SPA
    renders them through the same per-finding path as a validation issue, and any
    other client can act on them, instead of parsing one flattened string.

    ``detail`` is the same flattened one-liner ``order.blocked_message`` gives the
    CLI, kept because every 4xx on this API carries a ``detail`` string (FastAPI's
    ``HTTPException`` shape) and generic error handling reads it. ``findings`` is
    EMPTY when the spec never reached the materializer (a structurally invalid
    project — the gate's own 422), which is exactly the truth: nothing found it
    unorderable, it was never ordered."""

    detail: str
    findings: list[OrderFinding]
