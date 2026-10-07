"""The project-write surface's reg_meta connection helper (``routes/project.py``).

``/api/project/validate`` and ``/api/project/order`` are thin adapters over
reg_meta's shared ``semantic.validate_project`` and ``order.materialize_order``;
the one thing they own here is HOW the selected artifact is opened.

``per_request_conn`` is the LOCKED cross-thread-safety pattern: open + query +
close on ONE thread (the threadpool worker), as a plain ``with`` and NEVER a
generator ``Depends`` (which can run on a different AnyIO thread →
``sqlite3.ProgrammingError``).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

import reg_meta.db

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator
    from pathlib import Path


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
