"""Query functions for the documentation index."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

# Shared FTS5 MATCH-expression builder (quoted prefix terms): same safety the
# main `search` uses, so a raw user query with FTS operators can't raise on the
# doc index either. Same-package internal helper.
from .doc_db import RelatedDocument, RelatedDocumentContent
from .queries import _fts_match_query

if TYPE_CHECKING:
    import sqlite3


_RELATED_DOCUMENT_COLUMNS = (
    "title, filename, source_url, license, fetched, sha256, byte_size"
)


def _related_document_from_row(row: sqlite3.Row) -> RelatedDocument:
    return RelatedDocument(
        title=row["title"],
        filename=row["filename"],
        source_url=row["source_url"],
        license=row["license"],
        fetched=row["fetched"],
        sha256=row["sha256"],
        byte_size=row["byte_size"],
    )


def related_documents_for_register(
    conn: sqlite3.Connection, register: str
) -> tuple[RelatedDocument, ...]:
    """List rehosted register-version related documents for one register."""
    rows = conn.execute(
        f"SELECT {_RELATED_DOCUMENT_COLUMNS} "
        "FROM related_document "
        "WHERE register = ? "
        "ORDER BY id",
        (register,),
    ).fetchall()
    return tuple(_related_document_from_row(row) for row in rows)


def related_document_content(
    conn: sqlite3.Connection, register: str, filename: str
) -> RelatedDocumentContent | None:
    """Fetch one rehosted related-document binary by its register-local filename."""
    row = conn.execute(
        f"SELECT {_RELATED_DOCUMENT_COLUMNS}, content "
        "FROM related_document "
        "WHERE register = ? AND filename = ?",
        (register, filename),
    ).fetchone()
    if row is None:
        return None
    return RelatedDocumentContent(
        **_related_document_from_row(row).model_dump(),
        content=row["content"],
    )


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

    The raw user query is rewritten to a safe FTS5 MATCH expression (quoted
    prefix terms) so stray FTS syntax can't raise; a query with no searchable
    token returns no results (rather than an empty-MATCH error).

    Returns {"total_count": int, "results": [...]}.
    """
    match_query = _fts_match_query(query)
    if match_query is None:
        return {"total_count": 0, "results": []}

    where_parts = ["doc_fts MATCH ?"]
    params: list[object] = [match_query]

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
               d.source, d.source_url, d.source_title, rank,
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
                # The SCB source-document identifier the doc was derived from —
                # a pointer for consumers that link out instead of republishing.
                "source": row["source"],
                # The resolved SCB-PDF link + human title for `source`, populated
                # at doc-DB build from the curated map (#372); None when uncurated.
                "source_url": row["source_url"],
                "source_title": row["source_title"],
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
        # Resolved SCB-PDF link + human title (#372), populated at doc-DB build
        # from the curated map; None when uncurated.
        "source_url": row["source_url"],
        "source_title": row["source_title"],
        "body": row["body"],
        # The link-stripped, marker-free plain text — consumers that serve an
        # EXCERPT (not full-text) build it from this rather than the raw `body`.
        "body_clean": row["body_clean"],
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


def doc_registers(conn: sqlite3.Connection) -> set[str]:
    """The set of registers that have ANY ingested documentation.

    Lets a caller distinguish "no docs ingested for this register" (the register
    isn't in this set — coverage is LISA-only today) from "no docs found for this
    specific variable" (register present, but no matching doc)."""
    return {r["register"] for r in conn.execute("SELECT DISTINCT register FROM doc")}
