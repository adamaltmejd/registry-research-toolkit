"""Derived tables: pure functions of a built artifact's core graph.

Derive drops and recomputes its tables in a fixed insertion order, so identical
inputs give identical bytes. `validate_built_db` recomputes them and requires
equality. `derive_artifact` is the standalone step over a published artifact.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

# Bootstrap by moving (RUST_RUNTIME_SPEC.md section 4): the resolver rules are read
# from the reader, so the derived rows equal what it returns by construction.
from reg_meta.catalog import Catalog
from reg_meta.db import get_manifest, register_py_lower
from reg_meta.errors import RegMetaError

from .artifact_identity import builder_commit, generation_id
from .db import (
    DERIVED_DDL,
    SCHEMA_VERSION,
    _unlink_wal_sidecars,
    open_built_db,
    publish_db,
)

if TYPE_CHECKING:
    from pathlib import Path

# simplify: worker startup costs more than the work below this many pairs, so
# fixtures stay in-process. The pinned global artifact has ~71k pairs: 88 s on one
# core, 31 s on ten. Replace the moved resolver with set-based SQL if G1 needs more.
_PARALLEL_MIN_PAIRS = 5_000

_worker_catalog: Catalog | None = None


def _catalog(conn: sqlite3.Connection) -> Catalog:
    conn.row_factory = sqlite3.Row
    register_py_lower(conn)
    return Catalog(conn)


def _emitted(
    catalog: Catalog, pairs: list[tuple[int, int]]
) -> list[tuple[int, int, str, str]]:
    return [
        (variable_id, variant_id, name.lower(), name)
        for variable_id, variant_id in pairs
        for name in sorted(
            catalog.delivery_columns(variable_id, variant_id), key=str.lower
        )
    ]


def _open_worker(path: str) -> None:
    global _worker_catalog
    # A plain connection, never immutable: an extend-db overlay lives in the WAL.
    _worker_catalog = _catalog(sqlite3.connect(path))


def _worker_emitted(pairs: list[tuple[int, int]]) -> list[tuple[int, int, str, str]]:
    assert _worker_catalog is not None
    return _emitted(_worker_catalog, pairs)


def resolver_columns(conn: sqlite3.Connection) -> list[tuple[int, int, str, str]]:
    """Whole-history resolver-emitted delivery columns per (variable, variant).

    Rows are `(variable_id, register_variant_id, lowercase name, canonical
    spelling)` in primary-key order. A pair without a base state emits nothing,
    so the pairs come from `variable_state`. A large artifact is computed by
    worker processes over its committed rows.
    """
    pairs = [
        (variable_id, variant_id)
        for variable_id, variant_id in conn.execute(
            "SELECT DISTINCT variable_id, register_variant_id FROM variable_state "
            "ORDER BY variable_id, register_variant_id"
        )
    ]
    path = next(
        row[2] for row in conn.execute("PRAGMA database_list") if row[1] == "main"
    )
    if len(pairs) < _PARALLEL_MIN_PAIRS or not path:
        factory = conn.row_factory
        try:
            return _emitted(_catalog(conn), pairs)
        finally:
            conn.row_factory = factory
    if conn.in_transaction:
        raise ValueError("Derive workers read committed rows; commit before deriving")
    workers = os.process_cpu_count() or 1
    size = -(-len(pairs) // (4 * workers))
    chunks = [pairs[start : start + size] for start in range(0, len(pairs), size)]
    with ProcessPoolExecutor(
        workers, initializer=_open_worker, initargs=(path,)
    ) as pool:
        return [row for rows in pool.map(_worker_emitted, chunks) for row in rows]


def derive(conn: sqlite3.Connection) -> None:
    """Recompute every derived table from the committed core graph on `conn`."""
    rows = resolver_columns(conn)
    conn.execute("DELETE FROM resolver_column")
    conn.executemany(
        "INSERT INTO resolver_column (variable_id, register_variant_id, "
        "delivery_column_lower, delivery_column_name) VALUES (?, ?, ?, ?)",
        rows,
    )


def derive_artifact(base: Path, out: Path) -> None:
    """Publish a derived copy of the artifact `base` at `out`; `base` is only read.

    The copy takes this builder's schema version and commit, records the base's
    generation as `derived_from_generation_id`, keeps every other identity key and
    recomputes `generation_id`. A steward copy derives over its full global-plus-
    overlay graph; its physical holdings are left as compiled. The copy must pass
    `validate_built_db` before it replaces `out` atomically.
    """
    from .validate import validate_built_db

    if out.resolve() == base.resolve():
        raise ValueError("Derive writes a copy; --out must differ from --base")
    revision = builder_commit()
    tmp = out.with_name(out.name + ".tmp")
    tmp.unlink(missing_ok=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(base, tmp)
    try:
        # Every read comes from the copy, so the gate sees the bytes it derives.
        try:
            with closing(open_built_db(tmp, older_minor=True)) as conn:
                manifest = get_manifest(conn)
        except RegMetaError as exc:
            # The gate reads the copy; name the file the user passed.
            raise replace(
                exc,
                message=exc.message.replace(str(tmp), str(base)),
                remediation="Derive admits this builder's schema major with an "
                "equal or older minor; rebuild the base otherwise.",
            ) from None
        kind = manifest.get("catalog_artifact_kind")
        if kind not in {"catalog", "steward"} or "generation_id" not in manifest:
            raise ValueError(
                "Derive requires a published catalog or steward artifact with a "
                "generation_id"
            )
        with closing(sqlite3.connect(tmp)) as conn:
            conn.executescript(DERIVED_DDL)
            derive(conn)
            manifest.update(
                schema_version=SCHEMA_VERSION,
                builder_commit=revision,
                derived_from_generation_id=manifest["generation_id"],
            )
            manifest["generation_id"] = generation_id(manifest)
            conn.executemany(
                "INSERT OR REPLACE INTO import_manifest(key, value) VALUES (?, ?)",
                sorted(manifest.items()),
            )
            # The base's statistics stay valid; only the derived tables are new.
            conn.execute("ANALYZE resolver_column")
            conn.commit()
        validation = validate_built_db(tmp, flavored=kind == "steward")
        if not validation.passed:
            raise ValueError(
                "Derived artifact validation failed: " + "; ".join(validation.failures)
            )
        if builder_commit() != revision:
            raise ValueError("Builder revision changed during derive")
        publish_db(tmp, out)
    except BaseException:
        tmp.unlink(missing_ok=True)
        _unlink_wal_sidecars(tmp)
        raise
