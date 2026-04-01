"""Doc index: build and manage the FTS5 search index for parsed documentation."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

from .errors import EXIT_CONFIG, RegmetaError

# No logging.basicConfig here -- messages surface only when the caller
# (e.g. CLI --verbose) configures a handler.  This is intentional for
# CLI feedback that should not appear in quiet/programmatic usage.
log = logging.getLogger(__name__)

DOC_DB_FILENAME = "regmeta_docs.db"

DOC_DDL = """\
CREATE TABLE IF NOT EXISTS doc (
    doc_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    register     TEXT NOT NULL,
    filename     TEXT NOT NULL UNIQUE,
    variable     TEXT,
    display_name TEXT NOT NULL,
    tags         TEXT NOT NULL,
    source       TEXT,
    body         TEXT NOT NULL,
    body_clean   TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS doc_fts USING fts5(
    display_name, variable, body_clean,
    content='doc', content_rowid='doc_id',
    tokenize='unicode61'
);

CREATE INDEX IF NOT EXISTS idx_doc_variable ON doc(variable);
CREATE INDEX IF NOT EXISTS idx_doc_filename ON doc(filename);

CREATE TABLE IF NOT EXISTS doc_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Frontmatter parser (no PyYAML dependency)
# ---------------------------------------------------------------------------

_FM_DELIM = re.compile(r"^---\s*$")


def parse_frontmatter(text: str) -> tuple[dict[str, object], str]:
    """Parse YAML frontmatter from markdown text.

    Returns (metadata_dict, body) where body is the text after frontmatter.
    Only handles the subset we generate: scalar values and simple lists.
    """
    lines = text.split("\n")
    if not lines or not _FM_DELIM.match(lines[0]):
        return {}, text

    end = None
    for i in range(1, len(lines)):
        if _FM_DELIM.match(lines[i]):
            end = i
            break
    if end is None:
        return {}, text

    meta: dict[str, object] = {}
    current_key: str | None = None
    current_list: list[str] | None = None

    for line in lines[1:end]:
        # List item: "  - value"
        if line.startswith("  - ") and current_key:
            if current_list is None:
                current_list = []
            current_list.append(line[4:].strip())
            continue

        # Save accumulated list
        if current_list is not None and current_key:
            meta[current_key] = current_list
            current_list = None

        # Key-value: "key: value"
        if ":" in line:
            key, _, val = line.partition(":")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            current_key = key
            if val:
                meta[key] = val
            # If val is empty, next lines might be a list
        else:
            current_key = None

    if current_list is not None and current_key:
        meta[current_key] = current_list

    body = "\n".join(lines[end + 1 :]).lstrip("\n")
    return meta, body


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def doc_db_path(db_arg: str | None) -> Path:
    """Resolve path to the doc index DB."""
    from .db import db_path_from_args

    return db_path_from_args(db_arg, filename=DOC_DB_FILENAME)


def bundled_docs_dir() -> Path | None:
    """Find the bundled docs directory shipped with the package."""
    # Walk up from this file to find regmeta/docs/
    pkg_dir = Path(__file__).resolve().parent
    candidates = [
        pkg_dir / "docs",  # installed package: docs bundled inside the package dir
        pkg_dir.parent.parent
        / "docs",  # development layout: src/regmeta/../../docs → regmeta/docs/
    ]
    for candidate in candidates:
        if candidate.is_dir() and any(candidate.iterdir()):
            return candidate
    return None


def open_doc_db(db_path: Path) -> sqlite3.Connection:
    """Open the doc index DB read-only."""
    from .db import open_db

    return open_db(
        db_path,
        error_code="doc_db_not_found",
        remediation="Run `regmeta maintain build-docs` to build the doc search index.",
    )


def ensure_doc_db(db_arg: str | None) -> sqlite3.Connection:
    """Open the doc DB, auto-building from bundled docs if missing."""
    path = doc_db_path(db_arg)
    if not path.exists():
        docs_dir = bundled_docs_dir()
        if docs_dir is None:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="doc_db_not_found",
                error_class="configuration",
                message=f"Doc index not found: {path}",
                remediation="Run `regmeta maintain build-docs --docs-dir <path>` to build the index.",
            )
        log.info("Building doc index from %s ...", docs_dir)
        build_doc_db(docs_dir, path.parent)
    return open_doc_db(path)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _clean_body_for_search(body: str) -> str:
    """Strip markdown formatting from body text for cleaner FTS snippets.

    Removes tables, wiki-links, bold/italic markers, URLs, and other
    formatting noise while preserving the prose content.
    """
    lines = []
    for line in body.split("\n"):
        stripped = line.strip()
        # Skip table rows and separator lines
        if stripped.startswith("|") or stripped.startswith("---"):
            continue
        # Skip image references
        if stripped.startswith("![]") or stripped.startswith("Image "):
            continue
        # Skip empty bold-only lines (variable headers)
        if re.match(r"^\*\*[^*]+\*\*\s*$", stripped):
            continue
        lines.append(line)

    text = "\n".join(lines)
    # Strip wiki-links: [[Name]] → Name
    text = re.sub(r"\[\[([^\]]+)\]\]", r"\1", text)
    # Strip markdown links: [text](url) → text
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    # Strip bold/italic markers
    text = re.sub(r"\*{1,3}([^*]+)\*{1,3}", r"\1", text)
    # Strip heading markers
    text = re.sub(r"^#{1,4}\s+", "", text, flags=re.MULTILINE)
    # Collapse whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def build_doc_db(docs_dir: Path, db_dir: Path) -> Path:
    """Build the doc search index from markdown files.

    Scans docs_dir for register subdirectories (e.g. lisa/),
    parses frontmatter from each .md file, and populates the
    FTS5 index.

    Returns the path to the created DB.
    """
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / DOC_DB_FILENAME

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    conn.executescript(DOC_DDL)

    total = 0
    for register_dir in sorted(docs_dir.iterdir()):
        if not register_dir.is_dir():
            continue
        register = register_dir.name
        for md_file in sorted(register_dir.glob("*.md")):
            text = md_file.read_text(encoding="utf-8")
            meta, body = parse_frontmatter(text)

            if not body.strip():
                continue

            tags = meta.get("tags", [])
            if isinstance(tags, str):
                tags = [tags]

            body_clean = _clean_body_for_search(body)
            conn.execute(
                "INSERT INTO doc (register, filename, variable, display_name, tags, source, body, body_clean) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    register,
                    md_file.name,
                    meta.get("variable"),
                    meta.get("display_name", md_file.stem),
                    json.dumps(tags, ensure_ascii=False),
                    meta.get("source"),
                    body,
                    body_clean,
                ),
            )
            total += 1

    # Populate FTS index
    conn.execute("INSERT INTO doc_fts(doc_fts) VALUES('rebuild')")

    # Store metadata
    conn.execute(
        "INSERT INTO doc_meta (key, value) VALUES ('doc_count', ?)",
        (str(total),),
    )
    conn.execute(
        "INSERT INTO doc_meta (key, value) VALUES ('docs_dir', ?)",
        (str(docs_dir.resolve()),),
    )

    conn.commit()
    conn.close()
    log.info("Indexed %d docs from %s → %s", total, docs_dir, db_path)
    return db_path
