"""The per-request reg_meta read-only connection (shared by the read routes).

A shared ``sqlite3`` connection is NOT safe across FastAPI's sync-handler
threadpool (per-connection cursor-state races), so every read handler opens a
FRESH read-only connection per request via ``catalog_conn`` — used as a plain
``with`` INSIDE the sync handler body, NOT a FastAPI ``Depends`` (a sync
endpoint's generator *dependency* is entered on a possibly-DIFFERENT threadpool
thread than the handler, so a dependency-opened sqlite connection gets used
cross-thread → intermittent ``sqlite3.ProgrammingError`` under concurrency; this
was Codex P1 on #168, reproduced 72/80 before the fix). Opening within the
handler body keeps open + query + close on one thread.

It opens from the boot-resolved ``app.state.db_path`` with ``check_schema=False``
— the lifespan (``app.py``) already validated the schema at boot. Both
``routes/catalog.py`` and ``routes/search.py`` use it; it lives here (not in
``routes/catalog.py``) so the search router doesn't import a sibling route module
just for the connection seam.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

import reg_meta.db
import reg_meta.doc_db

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator

    from fastapi import Request


@contextmanager
def catalog_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """A per-request reg_meta read-only connection, opened ON THE CALLING THREAD.

    See the module docstring for the threadpool rationale. ``check_schema=False``:
    the lifespan already validated the schema at boot."""
    conn = reg_meta.db.open_db(request.app.state.db_path, check_schema=False)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def docs_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """A per-request read-only connection to the docs DB (#354), same
    one-thread-per-request model as ``catalog_conn``. The caller MUST first
    check ``request.app.state.docs_db_path is not None`` — the docs DB is
    optional (the boot seam sets the path to None when absent / schema-incompat,
    and the docs routes return a "not ingested" response in that case rather than
    opening). ``check_schema=False``: the lifespan already validated it."""
    conn = reg_meta.doc_db.open_doc_db(
        request.app.state.docs_db_path, check_schema=False
    )
    try:
        yield conn
    finally:
        conn.close()
