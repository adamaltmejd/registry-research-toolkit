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

import reg_core_py

# Bootstrap by moving (RUST_RUNTIME_SPEC.md section 4): the resolver rules are read
# from the reader, so the derived rows equal what it returns by construction.
from reg_meta.catalog import Catalog
from reg_meta.db import get_manifest, register_py_lower
from reg_meta.errors import RegMetaError

from .artifact_identity import builder_commit, generation_id
from .db import (
    _VALUE_CODE_STOPLIST_EXACT,
    _VALUE_CODE_STOPLIST_PREFIXES,
    DERIVED_DDL,
    SCHEMA_VERSION,
    SEARCH_INDEX_DDL,
    _progress,
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


def _sql_text(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


# value_code_fts leaves out stoplisted labels (db.py) and, since #478, ownerless
# codes: `mapping_count = 0` means no variable owner, and classification-owned
# codes stay searchable through `classification_code`. This mirrors the reader's
# owner definition in reg_meta/queries.py `_code_owner_annotations_batch`, which
# the unscoped value arm cannot apply before its page is cut.
_VALUE_CODE_LISTED = (
    f"label NOT IN ({', '.join(map(_sql_text, sorted(_VALUE_CODE_STOPLIST_EXACT)))})"
    " AND NOT ("
    + " OR ".join(
        f"label LIKE {_sql_text(p + '%')}" for p in _VALUE_CODE_STOPLIST_PREFIXES
    )
    + ")"
)
_VALUE_CODE_OWNED = (
    "(mapping_count > 0 OR EXISTS (SELECT 1 FROM classification_code cc "
    "WHERE cc.code_id = value_code.code_id))"
)

# Each search index and the rows it holds: rowid, then its columns in declaration
# order. `validate_built_db` compares the stored rows with the same source.
SEARCH_INDEXES = {
    "register_fts": "SELECT register_id, register_id, fold_search(name), "
    "fold_search(purpose) FROM register",
    "variable_fts": "SELECT variable_id, register_id, fold_search(provider_key), "
    "fold_search(name), fold_search(definition), fold_search(description), "
    "fold_search(operational_definition), fold_search(delivery_column_names) "
    "FROM variable_search_text",
    "classification_fts": "SELECT id, fold_search(short_name), fold_search(name), "
    "fold_search(name_en), fold_search(description) FROM classification",
    "value_code_fts": "SELECT code_id, fold_search(label) FROM value_code "
    f"WHERE {_VALUE_CODE_LISTED} AND {_VALUE_CODE_OWNED}",
}


def _fold_search(value: str | None) -> str | None:
    return None if value is None else reg_core_py.fold_search(value)


def register_fold_search(conn: sqlite3.Connection) -> None:
    """Register `reg-core`'s `fold_search` as the NULL-preserving SQL `fold_search`."""
    conn.create_function("fold_search", 1, _fold_search, deterministic=True)


def derive_search_indexes(conn: sqlite3.Connection) -> None:
    """Drop and refill the full-text indexes with `fold_search` text of the core graph.

    Dropping also replaces an older base's external-content indexes and their
    `variable_fts_content` view, whose name the new index's shadow table takes.
    Commits.
    """
    # simplify: a steward extension refolds the inherited value-code labels its
    # overlay never changes; skip value_code_fts there if derive misses its budget.
    _progress("Building search indexes...")
    register_fold_search(conn)
    for table in SEARCH_INDEXES:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute("DROP VIEW IF EXISTS variable_fts_content")
    conn.execute("DROP VIEW IF EXISTS variable_search_text")
    conn.executescript(SEARCH_INDEX_DDL)
    for table, source in SEARCH_INDEXES.items():
        columns = ", ".join(
            row[1] for row in conn.execute(f"PRAGMA table_info({table})")
        )
        conn.execute(f"INSERT INTO {table} (rowid, {columns}) {source} ORDER BY 1")
    (n_ownerless,) = conn.execute(
        f"SELECT COUNT(*) FROM value_code WHERE {_VALUE_CODE_LISTED} "
        f"AND NOT {_VALUE_CODE_OWNED}"
    ).fetchone()
    if n_ownerless:
        _progress(
            f"  {n_ownerless:,} context-less value_codes excluded from value search (#478)"
        )
    conn.commit()


def derive(conn: sqlite3.Connection) -> None:
    """Recompute every derived table from the committed core graph on `conn`."""
    rows = resolver_columns(conn)
    conn.execute("DELETE FROM resolver_column")
    conn.executemany(
        "INSERT INTO resolver_column (variable_id, register_variant_id, "
        "delivery_column_lower, delivery_column_name) VALUES (?, ?, ?, ?)",
        rows,
    )
    derive_search_indexes(conn)


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
