"""Query functions for the documentation index."""

from __future__ import annotations

import json
import sqlite3


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

    Returns {"total_count": int, "results": [...], "docs_dir": str}.
    """
    # Build the WHERE clause
    where_parts = ["doc_fts MATCH ?"]
    params: list[object] = [query]

    if register:
        where_parts.append("d.register = ?")
        params.append(register)

    if type_tag:
        tag = type_tag if type_tag.startswith("type/") else f"type/{type_tag}"
        where_parts.append(
            "d.doc_id IN (SELECT d2.doc_id FROM doc d2, json_each(d2.tags) je WHERE je.value = ?)"
        )
        params.append(tag)

    if topic_tag:
        tag = topic_tag if topic_tag.startswith("topic/") else f"topic/{topic_tag}"
        where_parts.append(
            "d.doc_id IN (SELECT d2.doc_id FROM doc d2, json_each(d2.tags) je WHERE je.value = ?)"
        )
        params.append(tag)

    where = " AND ".join(where_parts)

    # Count
    count_sql = f"SELECT count(*) FROM doc_fts JOIN doc d ON d.doc_id = doc_fts.rowid WHERE {where}"
    total = conn.execute(count_sql, params).fetchone()[0]

    # Results with snippets
    result_sql = f"""
        SELECT d.filename, d.register, d.variable, d.display_name, d.tags,
               rank,
               snippet(doc_fts, 2, '**', '**', '…', 24) AS snippet
        FROM doc_fts
        JOIN doc d ON d.doc_id = doc_fts.rowid
        WHERE {where}
        ORDER BY rank
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    rows = conn.execute(result_sql, params).fetchall()

    docs_dir = _get_docs_dir(conn)

    return {
        "total_count": total,
        "docs_dir": docs_dir,
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
    # Try variable match first
    row = conn.execute(
        "SELECT * FROM doc WHERE variable = ? LIMIT 1", (identifier,)
    ).fetchone()

    # Try filename (with and without .md)
    if row is None:
        row = conn.execute(
            "SELECT * FROM doc WHERE filename = ? OR filename = ? LIMIT 1",
            (identifier, f"{identifier}.md"),
        ).fetchone()

    if row is None:
        return None

    docs_dir = _get_docs_dir(conn)

    return {
        "filename": row["filename"],
        "register": row["register"],
        "variable": row["variable"],
        "display_name": row["display_name"],
        "tags": json.loads(row["tags"]),
        "source": row["source"],
        "body": row["body"],
        "file_path": f"{docs_dir}/{row['register']}/{row['filename']}" if docs_dir else None,
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
    docs_dir = _get_docs_dir(conn)

    if not type_tag and not topic_tag and not register:
        return _doc_list_summary(conn, docs_dir)

    where_parts = ["1=1"]
    params: list[object] = []

    if register:
        where_parts.append("d.register = ?")
        params.append(register)
    if type_tag:
        tag = type_tag if type_tag.startswith("type/") else f"type/{type_tag}"
        where_parts.append(
            "d.doc_id IN (SELECT d2.doc_id FROM doc d2, json_each(d2.tags) je WHERE je.value = ?)"
        )
        params.append(tag)
    if topic_tag:
        tag = topic_tag if topic_tag.startswith("topic/") else f"topic/{topic_tag}"
        where_parts.append(
            "d.doc_id IN (SELECT d2.doc_id FROM doc d2, json_each(d2.tags) je WHERE je.value = ?)"
        )
        params.append(tag)

    where = " AND ".join(where_parts)
    rows = conn.execute(
        f"SELECT filename, variable, display_name, tags FROM doc d WHERE {where} ORDER BY filename",
        params,
    ).fetchall()

    return {
        "total_count": len(rows),
        "docs_dir": docs_dir,
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


def _doc_list_summary(conn: sqlite3.Connection, docs_dir: str | None) -> dict:
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
        "docs_dir": docs_dir,
        "registers": {r["register"]: r["n"] for r in registers},
        "types": type_tags,
        "topics": topic_tags,
        "total_count": sum(r["n"] for r in registers),
    }


def doc_exists(conn: sqlite3.Connection, variable: str) -> bool:
    """Check whether documentation exists for a variable."""
    row = conn.execute(
        "SELECT 1 FROM doc WHERE variable = ? LIMIT 1", (variable,)
    ).fetchone()
    return row is not None


def _get_docs_dir(conn: sqlite3.Connection) -> str | None:
    """Get the source docs directory from the doc DB metadata."""
    row = conn.execute(
        "SELECT value FROM doc_meta WHERE key = 'docs_dir'"
    ).fetchone()
    return row["value"] if row else None
