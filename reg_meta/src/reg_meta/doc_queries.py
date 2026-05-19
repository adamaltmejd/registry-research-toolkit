"""Query functions for the documentation index."""

from __future__ import annotations

import json
import sqlite3


def _add_tag_filter(
    where_parts: list[str], params: list[object], prefix: str, value: str
) -> None:
    """Append a tag filter clause for the given prefix (e.g. 'type', 'topic')."""
    tag = value if value.startswith(f"{prefix}/") else f"{prefix}/{value}"
    where_parts.append(
        "d.doc_id IN (SELECT d2.doc_id FROM doc d2, json_each(d2.tags) je WHERE je.value = ?)"
    )
    params.append(tag)


def doc_search(
    conn: sqlite3.Connection,
    query: str,
    *,
    type_tag: str | None = None,
    topic_tag: str | None = None,
    register: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict:
    """FTS5 search over documentation.

    Returns {"total_count": int, "results": [...]}.
    """
    where_parts = ["doc_fts MATCH ?"]
    params: list[object] = [query]

    if register:
        where_parts.append("d.register = ?")
        params.append(register)
    if type_tag:
        _add_tag_filter(where_parts, params, "type", type_tag)
    if topic_tag:
        _add_tag_filter(where_parts, params, "topic", topic_tag)

    where = " AND ".join(where_parts)

    count_sql = f"""
        SELECT count(*) AS total_count
        FROM doc_fts
        JOIN doc d ON d.doc_id = doc_fts.rowid
        WHERE {where}
    """
    total = conn.execute(count_sql, params).fetchone()["total_count"]

    sql = f"""
        SELECT d.filename, d.register, d.variable, d.display_name, d.tags,
               rank,
               snippet(doc_fts, 2, '**', '**', '…', 24) AS snippet
        FROM doc_fts
        JOIN doc d ON d.doc_id = doc_fts.rowid
        WHERE {where}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(sql, [*params, limit, offset]).fetchall()

    return {
        "total_count": total,
        "results": [
            {
                "type": "doc",
                "filename": row["filename"],
                "register": row["register"],
                "variable": row["variable"],
                "display_name": row["display_name"],
                "tags": json.loads(row["tags"]),
                "fts_rank": row["rank"],
                "snippet": row["snippet"],
            }
            for row in rows
        ],
    }


def doc_get(
    conn: sqlite3.Connection,
    identifier: str,
) -> dict | None:
    """Retrieve a doc by variable name or filename.

    Returns the full doc record or None.
    """
    # Try variable match first (case-insensitive)
    row = conn.execute(
        "SELECT * FROM doc WHERE variable = ? COLLATE NOCASE LIMIT 1",
        (identifier,),
    ).fetchone()

    # Try filename (with and without .md, case-insensitive)
    if row is None:
        row = conn.execute(
            "SELECT * FROM doc WHERE filename = ? COLLATE NOCASE "
            "OR filename = ? COLLATE NOCASE LIMIT 1",
            (identifier, f"{identifier}.md"),
        ).fetchone()

    if row is None:
        return None

    return {
        "filename": row["filename"],
        "register": row["register"],
        "variable": row["variable"],
        "display_name": row["display_name"],
        "tags": json.loads(row["tags"]),
        "source": row["source"],
        "body": row["body"],
    }


def doc_list(
    conn: sqlite3.Connection,
    *,
    type_tag: str | None = None,
    topic_tag: str | None = None,
    register: str | None = None,
) -> dict:
    """List available docs with optional filtering.

    Without filters, returns summary stats.
    With filters, returns matching doc records.
    """
    if not type_tag and not topic_tag and not register:
        return _doc_list_summary(conn)

    where_parts = ["1=1"]
    params: list[object] = []

    if register:
        where_parts.append("d.register = ?")
        params.append(register)
    if type_tag:
        _add_tag_filter(where_parts, params, "type", type_tag)
    if topic_tag:
        _add_tag_filter(where_parts, params, "topic", topic_tag)

    where = " AND ".join(where_parts)
    rows = conn.execute(
        f"SELECT filename, variable, display_name, tags FROM doc d WHERE {where} ORDER BY filename",
        params,
    ).fetchall()

    return {
        "total_count": len(rows),
        "results": [
            {
                "filename": r["filename"],
                "variable": r["variable"],
                "display_name": r["display_name"],
                "tags": json.loads(r["tags"]),
            }
            for r in rows
        ],
    }


def _doc_list_summary(conn: sqlite3.Connection) -> dict:
    """Summary stats: counts by register, type, and topic."""
    registers = conn.execute(
        "SELECT register, count(*) as n FROM doc GROUP BY register ORDER BY register"
    ).fetchall()

    tags = conn.execute(
        "SELECT je.value as tag, count(*) as n FROM doc, json_each(doc.tags) je GROUP BY je.value ORDER BY n DESC"
    ).fetchall()

    type_tags = {r["tag"]: r["n"] for r in tags if r["tag"].startswith("type/")}
    topic_tags = {r["tag"]: r["n"] for r in tags if r["tag"].startswith("topic/")}

    return {
        "registers": {r["register"]: r["n"] for r in registers},
        "types": type_tags,
        "topics": topic_tags,
        "total_count": sum(r["n"] for r in registers),
    }


def doc_exists(conn: sqlite3.Connection, variable: str) -> bool:
    """Check whether documentation exists for a variable."""
    row = conn.execute(
        "SELECT 1 FROM doc WHERE variable = ? COLLATE NOCASE LIMIT 1",
        (variable,),
    ).fetchone()
    return row is not None
