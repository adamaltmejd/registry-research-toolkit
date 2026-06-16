"""Shared project-write validation composition (``routes/project.py`` +
``routes/kit.py``).

The reg_meta-backed write endpoints (``/api/project/validate`` and ``/api/kit``)
run the SAME §6.8.0 layered composition over a raw ``project_data.json`` — the
namespaced-block layer + the reg_meta-backed semantic layer + the build-side
cross-block referential checks — differing only in what they DO with the result
(``/validate`` returns a 200 diagnostic; ``/kit`` gates and then packages). This
module owns the shared pieces so a NEW layer is added in ONE place and can't
silently skip an endpoint (the drift this consolidation prevents). The
endpoint-specific bits stay in each route: the model-build error policy
(``/validate`` → a 200 issue; ``/kit`` → 422), the kit-only
``panel_inheritance_unresolvable`` check, and the response shape.

The reg_meta connection helper lives here too (``per_request_conn``) — the LOCKED
cross-thread-safety pattern: open + query + close on ONE thread (the threadpool
worker), as a plain ``with`` and NEVER a generator ``Depends`` (which can run on a
different AnyIO thread → ``sqlite3.ProgrammingError``).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import reg_meta.db
from reg_monabundle.build.spec_loader import binding_options_issues, block_issue

from reg_webapp.semantic import validate_semantic

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator
    from pathlib import Path

    from reg_meta.catalog import Catalog
    from reg_schema.project_data import ProjectData
    from reg_schema.validation import ValidationIssue

    from reg_webapp.catalog_index import CatalogIndex


@contextmanager
def per_request_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    """A per-request reg_meta read-only connection, opened ON THE CALLING THREAD
    (the threadpool worker running a route's offloaded blocking work).

    Used as a plain ``with`` (NOT a FastAPI ``Depends``) so open + query + close
    stay on ONE thread — the load-bearing cross-thread-safety property the
    A5.2a/b-i P1 established (a generator dependency can run on a possibly-different
    AnyIO threadpool thread → ``sqlite3.ProgrammingError`` under concurrency).
    ``check_schema=False``: the lifespan already validated the schema at boot."""
    conn = reg_meta.db.open_db(db_path, check_schema=False)
    try:
        yield conn
    finally:
        conn.close()


def block_issues(raw: dict[str, Any]) -> list[ValidationIssue]:
    """The ``reg_monabundle`` §6.8.2 block layer as an issue list. The code lives
    in its OWNER (``reg_monabundle.build.spec_loader.block_issue`` runs the
    amalgamation-safe raise-based ``validate_block`` and wraps its single raise as
    one canonical ``invalid_block`` issue, ``None`` when clean); an absent block
    validates trivially."""
    issue = block_issue(raw.get("reg_monabundle"))
    return [issue] if issue is not None else []


def semantic_issues(
    project: ProjectData,
    raw: dict[str, Any],
    catalog: Catalog,
    index: CatalogIndex | None,
) -> list[ValidationIssue]:
    """The §6.8.3 reg_meta-backed semantic layer (researcher caller) PLUS the
    build-time cross-block referential checks (orphan ``binding_options`` keys /
    suppress_k-on-non-categorical). Takes an already-built ``ProjectData`` (the
    caller owns the model-build error policy) and a live ``Catalog`` (the caller
    owns the connection lifetime). ``index`` is the deployment's steward
    ``CatalogIndex`` (``None`` for ``global``), threaded in for the column-based
    steward-admission warnings."""
    issues = list(
        validate_semantic(project, catalog, caller="researcher", index=index).issues
    )
    issues.extend(binding_options_issues(raw.get("reg_monabundle"), project))
    return issues
