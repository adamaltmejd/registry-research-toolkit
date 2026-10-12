"""Derived tables: pure functions of a built artifact's core graph.

Derive drops and recomputes its tables in a fixed insertion order, so identical
inputs give identical bytes. `validate_built_db` recomputes them and requires
equality. `derive_artifact` is the standalone step over a published artifact.
One module per table family, named by contract.
"""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

from reg_meta_build.artifact_identity import (
    builder_commit,
    generation_id,
    search_rows_sha256,
)
from reg_meta_build.db import (
    DERIVED_DDL,
    SCHEMA_VERSION,
    SEARCH_CURATION_DDL,
    _unlink_wal_sidecars,
    get_manifest,
    open_built_db,
    publish_db,
)
from reg_meta_build.derive.browse import browse_scopes, derive_browse
from reg_meta_build.derive.chains import CHAIN_TABLES, derive_chains
from reg_meta_build.derive.schema import derive_coded
from reg_meta_build.derive.search_index import derive_search_indexes
from reg_meta_build.derive.states import derive_states
from reg_meta_build.errors import RegMetaError

if TYPE_CHECKING:
    from pathlib import Path

# Every table DERIVED_DDL creates; the search indexes are analyzed with the base.
DERIVED_TABLES = (
    "expanded_state",
    "browse_delivery",
    "delivery_window",
    "resolver_column",
    *CHAIN_TABLES,
    "coded_variable_stats",
)


def derive(conn: sqlite3.Connection) -> None:
    """Recompute every derived table from the committed core graph on `conn`.

    Holdings-scope rows need a steward manifest and compiled holdings; extend-db
    compiles them after deriving, then adds those rows with `derive_holdings`.
    """
    derive_states(conn)
    scopes = browse_scopes(conn)
    derive_browse(conn, scopes)
    derive_coded(conn, scopes)
    derive_chains(conn)
    derive_search_indexes(conn)


def derive_holdings(conn: sqlite3.Connection) -> None:
    """Recompute the holdings-scope rows from a steward's compiled holdings."""
    derive_browse(conn, ("holdings",))
    derive_coded(conn, ("holdings",))


def derive_artifact(base: Path, out: Path) -> None:
    """Publish a derived copy of the artifact `base` at `out`; `base` is only read.

    The copy takes this builder's schema version and commit, records the base's
    generation as `derived_from_generation_id`, keeps every other identity key and
    recomputes `generation_id`. A steward copy derives over its full global-plus-
    overlay graph; its physical holdings are left as compiled. An older base gains
    empty `search_pin` and `search_synonym` tables and their empty-rows hashes, and
    a base older than 10.1 gains an all-NULL `classification_code.parent_code_id`. The copy must pass
    `validate_built_db` before it replaces `out` atomically.
    """
    from reg_meta_build.validate import validate_built_db

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
            conn.executescript(DERIVED_DDL + SEARCH_CURATION_DDL)
            # A pre-10.1 base has no code trees (#1297): its copy gains an all-NULL
            # parent_code_id and its analyzed index, as a fresh build has. ADD
            # COLUMN cannot carry the composite same-book key; with every value
            # NULL it constrains nothing, and validate_built_db checks same-book
            # parents by query, not by the declared key.
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(classification_code)")
            }
            if "parent_code_id" not in columns:
                conn.execute(
                    "ALTER TABLE classification_code ADD COLUMN parent_code_id INTEGER"
                )
                conn.execute(
                    "CREATE INDEX idx_classification_code_parent "
                    "ON classification_code(classification_id, parent_code_id)"
                )
                conn.execute("ANALYZE idx_classification_code_parent")
            derive(conn)
            manifest.setdefault("search_pins_sha256", search_rows_sha256(()))
            manifest.setdefault("search_synonyms_sha256", search_rows_sha256(()))
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
            # The base's statistics stay valid; only the derived tables and the
            # search index's text table and code index are new.
            for table in (
                *DERIVED_TABLES,
                "variable_search_text",
                "idx_value_code_code_nocase",
            ):
                conn.execute(f"ANALYZE {table}")
            conn.commit()
            # Dropping the base's derived tables and search indexes frees pages the
            # recomputed ones need not refill (validate refuses a freelist >= 1%),
            # as a fresh build's VACUUM reclaims them.
            conn.execute("VACUUM")
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
