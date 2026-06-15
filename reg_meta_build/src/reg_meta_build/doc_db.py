"""Build pipeline for the reg_meta doc index.

Parses frontmatter + markdown bodies under a curated docs directory and
writes the FTS5-indexed `reg_meta_docs.db`. Connection management and
schema-compat checks live in `reg_meta.doc_db`; this module imports the
shared constants and supplies the build entry point.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import tomllib
from pathlib import Path

from reg_meta.doc_db import DOC_DB_FILENAME, DOC_SCHEMA_VERSION

# No logging.basicConfig here -- messages surface only when the caller
# (e.g. CLI --verbose) configures a handler.  This is intentional for
# CLI feedback that should not appear in quiet/programmatic usage.
log = logging.getLogger(__name__)

DOC_DDL = """\
CREATE TABLE IF NOT EXISTS doc (
    doc_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    register     TEXT NOT NULL,
    filename     TEXT NOT NULL UNIQUE,
    variable     TEXT,
    display_name TEXT NOT NULL,
    tags         TEXT NOT NULL,
    source       TEXT,
    source_url   TEXT,
    source_title TEXT,
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
# Docs source dir (in-repo)
# ---------------------------------------------------------------------------


def repo_docs_dir() -> Path | None:
    """Return the in-repo source-markdown directory, for dev-time builds only.

    Runtime NEVER reads from this — users receive the prebuilt doc DB as a
    release asset via ``reg-meta update``. Only ``reg-meta-build build-docs``
    uses this, so a maintainer working from a checkout can rebuild the doc DB
    from ``reg_meta_build/docs/`` without passing ``--docs-dir`` every time.
    """
    pkg_dir = Path(__file__).resolve().parent
    candidate = pkg_dir.parent.parent / "docs"
    if candidate.is_dir() and any(candidate.iterdir()):
        return candidate
    return None


# ---------------------------------------------------------------------------
# Curated source → SCB-PDF map (#372)
# ---------------------------------------------------------------------------

DOC_SOURCES_FILE = "doc_sources.toml"


def load_doc_sources() -> dict[str, dict[str, str]]:
    """Load the curated `doc_sources.toml` map (#372).

    Returns {source_slug: {"url": ..., "title": ...}} keyed by the doc `source`
    slug with its trailing `.md` already stripped (the same canonical form
    `build_doc_db` looks up). Empty dict when the file is absent — wheel installs
    don't ship curation (it's a maintainer artifact, like the slug TOMLs), but a
    build from the in-repo docs MUST find it (the checkout ships it).
    """
    path = Path(__file__).resolve().parent.parent.parent / DOC_SOURCES_FILE
    if not path.is_file():
        return {}
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    sources = data.get("sources", {})
    return {
        slug: {"url": entry["url"], "title": entry["title"]}
        for slug, entry in sources.items()
    }


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

    source_map = load_doc_sources()
    unmapped_sources: set[str] = set()

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

            # Canonical source slug: strip a single trailing `.md` (most SCB
            # source slugs carry a stray one, a few don't) before looking it up
            # in the curated map (#372). The stored `source` is the canonical
            # form; `source_url`/`source_title` are None when uncurated.
            raw_source = meta.get("source")
            source = (
                raw_source.removesuffix(".md")
                if isinstance(raw_source, str)
                else raw_source
            )
            mapping = source_map.get(source) if isinstance(source, str) else None
            if isinstance(source, str) and mapping is None:
                unmapped_sources.add(source)

            body_clean = _clean_body_for_search(body)
            conn.execute(
                "INSERT INTO doc (register, filename, variable, display_name, tags, "
                "source, source_url, source_title, body, body_clean) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    register,
                    md_file.name,
                    meta.get("variable"),
                    meta.get("display_name", md_file.stem),
                    json.dumps(tags, ensure_ascii=False),
                    source,
                    mapping["url"] if mapping else None,
                    mapping["title"] if mapping else None,
                    body,
                    body_clean,
                ),
            )
            total += 1

    if unmapped_sources:
        # Coverage grows as new registers' docs land — an unmapped source is a
        # warning (the doc still indexes, just without a source link), NOT a
        # build failure.
        log.warning(
            "doc sources with no curated URL in %s: %s",
            DOC_SOURCES_FILE,
            ", ".join(sorted(unmapped_sources)),
        )

    # Populate FTS index
    conn.execute("INSERT INTO doc_fts(doc_fts) VALUES('rebuild')")

    # Store metadata
    for key, value in (
        ("schema_version", DOC_SCHEMA_VERSION),
        ("doc_count", str(total)),
    ):
        conn.execute(
            "INSERT INTO doc_meta (key, value) VALUES (?, ?)",
            (key, value),
        )

    conn.commit()
    conn.close()
    log.info("Indexed %d docs from %s → %s", total, docs_dir, db_path)
    return db_path
