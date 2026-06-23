"""Shared project-write validation composition (``routes/project.py``).

The reg_meta-backed ``/api/project/validate`` endpoint runs the §6.8.0 layered
composition over a raw ``project_data.json`` — the reg_schema structural layer +
the reg_meta-backed semantic layer — and returns a 200 diagnostic. This module
owns the shared semantic piece so a NEW layer is added in ONE place.

The reg_meta connection helper lives here too (``per_request_conn``) — the LOCKED
cross-thread-safety pattern: open + query + close on ONE thread (the threadpool
worker), as a plain ``with`` and NEVER a generator ``Depends`` (which can run on a
different AnyIO thread → ``sqlite3.ProgrammingError``).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

import reg_meta.db

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


def semantic_issues(
    project: ProjectData,
    catalog: Catalog,
    index: CatalogIndex | None,
) -> list[ValidationIssue]:
    """The §6.8.3 reg_meta-backed semantic layer (researcher caller). Takes an
    already-built ``ProjectData`` (the caller owns the model-build error policy)
    and a live ``Catalog`` (the caller owns the connection lifetime). ``index`` is
    the deployment's steward ``CatalogIndex`` (``None`` for ``global``), threaded in
    for the column-based steward-admission warnings."""
    return list(
        validate_semantic(project, catalog, caller="researcher", index=index).issues
    )
