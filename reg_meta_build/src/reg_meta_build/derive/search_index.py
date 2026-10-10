"""Full-text search indexes over `fold_search` text of the core graph."""

from __future__ import annotations

from typing import TYPE_CHECKING

import reg_core_py

from reg_meta_build.db import (
    _VALUE_CODE_STOPLIST_EXACT,
    _VALUE_CODE_STOPLIST_PREFIXES,
    SEARCH_INDEX_DDL,
)
from reg_meta_build.source_files import _progress

if TYPE_CHECKING:
    import sqlite3


def _sql_text(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


# value_code_fts leaves out stoplisted labels (db.py) and, since #478, ownerless
# codes: `mapping_count = 0` means no variable owner, and classification-owned
# codes stay searchable through `classification_code`. This mirrors the reader's
# owner definition (`crates/reg-catalog/src/ops/search/code.rs`), which
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

# The rows of `variable_search_text`, in its column order: a variable's own text, or
# else its state and alias-window texts, distinct and sorted, one per line.
# `validate_built_db` compares the stored rows with the same source.
VARIABLE_SEARCH_TEXT = """\
SELECT
    v.variable_id,
    v.register_id,
    v.provider_key,
    COALESCE(v.name, (
        SELECT group_concat(name, char(10)) FROM (
            SELECT name FROM variable_state WHERE variable_id = v.variable_id AND name IS NOT NULL
            UNION
            SELECT name FROM variable_alias_window WHERE variable_id = v.variable_id AND name IS NOT NULL
            ORDER BY name
        )
    )) AS name,
    COALESCE(v.definition, (
        SELECT group_concat(definition, char(10)) FROM (
            SELECT definition FROM variable_state WHERE variable_id = v.variable_id AND definition IS NOT NULL
            UNION
            SELECT definition FROM variable_alias_window WHERE variable_id = v.variable_id AND definition IS NOT NULL
            ORDER BY definition
        )
    )) AS definition,
    COALESCE(v.description, (
        SELECT group_concat(description, char(10)) FROM (
            SELECT description FROM variable_state WHERE variable_id = v.variable_id AND description IS NOT NULL
            UNION
            SELECT description FROM variable_alias_window WHERE variable_id = v.variable_id AND description IS NOT NULL
            ORDER BY description
        )
    )) AS description,
    v.operational_definition,
    (
        SELECT json_group_array(delivery_column_name)
        FROM (
            SELECT DISTINCT va.delivery_column_name
            FROM variable_alias va
            WHERE va.variable_id = v.variable_id
            ORDER BY va.delivery_column_name
        )
    ) AS delivery_column_names
FROM variable v"""

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
    `variable_fts_content` view, whose name the new index's shadow table takes, and
    its `variable_search_text` view with the table. Commits.
    """
    # simplify: a steward extension refolds the inherited value-code labels its
    # overlay never changes; skip value_code_fts there if derive misses its budget.
    _progress("Building search indexes...")
    register_fold_search(conn)
    for table in SEARCH_INDEXES:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute("DROP VIEW IF EXISTS variable_fts_content")
    # A view before 9.7, a table since; each DROP refuses the other kind.
    for (kind,) in conn.execute(
        "SELECT type FROM sqlite_master WHERE name = 'variable_search_text'"
    ).fetchall():
        conn.execute(f"DROP {kind.upper()} variable_search_text")
    conn.execute("DROP INDEX IF EXISTS idx_value_code_code_nocase")
    conn.executescript(SEARCH_INDEX_DDL)
    conn.execute(f"INSERT INTO variable_search_text {VARIABLE_SEARCH_TEXT} ORDER BY 1")
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
