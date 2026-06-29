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
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import NoReturn, cast

from reg_meta.doc_db import DOC_DB_FILENAME, DOC_SCHEMA_VERSION

from ._curation import curation_error

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

CREATE TABLE IF NOT EXISTS related_document (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    register   TEXT NOT NULL,
    title      TEXT NOT NULL,
    filename   TEXT NOT NULL,
    source_url TEXT NOT NULL,
    license    TEXT NOT NULL,
    fetched    TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    byte_size  INTEGER NOT NULL CHECK (byte_size >= 0),
    content    BLOB NOT NULL,
    UNIQUE(register, filename)
);

CREATE INDEX IF NOT EXISTS idx_related_document_register
    ON related_document(register);
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
RELATED_DOCUMENTS_FILE = "related_documents.toml"
RELATED_DOCUMENT_LICENSES = frozenset({"CC BY 4.0", "EU law"})


@dataclass(frozen=True)
class RelatedDocument:
    register: str
    title: str
    filename: str
    source_url: str
    license: str
    fetched: str


def _require_doc_curation_str(
    entry: dict[str, object],
    field: str,
    *,
    subject: str,
    code: str,
    remediation: str,
) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise curation_error(
            code=code,
            message=f"{subject} needs `{field}` as a non-empty string, got {value!r}.",
            remediation=remediation,
        )
    return value


def _require_doc_source_str(entry: dict, field: str, slug: str) -> str:
    return _require_doc_curation_str(
        entry,
        field,
        subject=f"doc_sources `{slug}`",
        code="doc_sources_invalid",
        remediation=f'Give `{field} = "<value>"` under `[sources."{slug}"]` in '
        "reg_meta_build/doc_sources.toml.",
    )


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
    out: dict[str, dict[str, str]] = {}
    for slug, entry in sources.items():
        if not isinstance(entry, dict):
            raise curation_error(
                code="doc_sources_invalid",
                message=f'doc_sources `{slug}` must be a `[sources."{slug}"]` '
                f"table, got {type(entry).__name__}.",
                remediation=f'Give a `[sources."{slug}"]` table with `url` / '
                "`title` in reg_meta_build/doc_sources.toml.",
            )
        out[slug] = {
            field: _require_doc_source_str(entry, field, slug)
            for field in ("url", "title")
        }
    return out


# ---------------------------------------------------------------------------
# Curated register-version related documents (#740)
# ---------------------------------------------------------------------------


def repo_related_documents_path() -> Path | None:
    path = Path(__file__).resolve().parent.parent.parent / RELATED_DOCUMENTS_FILE
    return path if path.is_file() else None


def repo_related_document_binaries_dir() -> Path | None:
    path = Path(__file__).resolve().parent.parent.parent / "input_data" / "SCB" / "docs"
    return path if path.is_dir() else None


def _related_documents_invalid(subject: str, message: str) -> NoReturn:
    raise curation_error(
        code="related_documents_invalid",
        message=f"{subject} {message}",
        remediation=(
            "Use `[[register.<slug>.document]]` entries with title, filename, "
            "source_url, license, and fetched in reg_meta_build/related_documents.toml."
        ),
    )


def _require_related_document_str(
    entry: dict[str, object], field: str, subject: str
) -> str:
    return _require_doc_curation_str(
        entry,
        field,
        subject=subject,
        code="related_documents_invalid",
        remediation=(
            f'Give `{field} = "<value>"` in the entry under '
            "reg_meta_build/related_documents.toml."
        ),
    )


def _parse_related_document(
    register: str, index: int, entry: dict[str, object]
) -> RelatedDocument:
    subject = f"related_documents `{register}` entry #{index}"
    expected = {"title", "filename", "source_url", "license", "fetched"}
    unknown = set(entry) - expected
    if unknown:
        _related_documents_invalid(
            subject, f"has unknown field(s): {', '.join(sorted(unknown))}."
        )

    filename = _require_related_document_str(entry, "filename", subject)
    if (
        Path(filename).name != filename
        or "/" in filename
        or "\\" in filename
        or filename in {".", ".."}
    ):
        _related_documents_invalid(
            subject,
            f"needs `filename` as a basename within the register's docs dir, got {filename!r}.",
        )

    fetched = _require_related_document_str(entry, "fetched", subject)
    try:
        date.fromisoformat(fetched)
    except ValueError as exc:
        raise curation_error(
            code="related_documents_invalid",
            message=f"{subject} needs `fetched` as YYYY-MM-DD, got {fetched!r}.",
            remediation=(
                "Use the date the maintainer fetched the binary, for example "
                '`fetched = "2026-06-23"`.'
            ),
        ) from exc

    return RelatedDocument(
        register=register,
        title=_require_related_document_str(entry, "title", subject),
        filename=filename,
        source_url=_require_related_document_str(entry, "source_url", subject),
        license=_parse_related_document_license(entry, subject),
        fetched=fetched,
    )


def _parse_related_document_license(entry: dict[str, object], subject: str) -> str:
    license_ = _require_related_document_str(entry, "license", subject)
    if license_ not in RELATED_DOCUMENT_LICENSES:
        _related_documents_invalid(
            subject,
            "needs `license` as one of "
            f"{', '.join(sorted(RELATED_DOCUMENT_LICENSES))}, got {license_!r}.",
        )
    return license_


def load_related_documents(
    path: Path | None = None,
) -> dict[str, list[RelatedDocument]]:
    """Load the curated register-version related-documents map (#740).

    Returns ``{register_slug: [RelatedDocument, ...]}``. The PDF binaries remain
    gitignored under ``input_data/SCB/docs/<register>/``; this tracked TOML is
    only the provenance and filename map.
    """
    if path is None:
        path = repo_related_documents_path()
    if path is None or not path.is_file():
        return {}

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    unknown_top_level = set(data) - {"register"}
    if unknown_top_level:
        _related_documents_invalid(
            "related_documents",
            f"has unknown top-level key(s): {', '.join(sorted(unknown_top_level))}.",
        )
    registers = data.get("register", {})
    if not isinstance(registers, dict):
        _related_documents_invalid(
            "related_documents", "needs top-level `[register.<slug>]` tables."
        )

    out: dict[str, list[RelatedDocument]] = {}
    seen: set[tuple[str, str]] = set()
    for register, block in sorted(registers.items()):
        if (
            not isinstance(register, str)
            or not register
            or Path(register).name != register
            or "/" in register
            or "\\" in register
            or register in {".", ".."}
        ):
            _related_documents_invalid(
                "related_documents", f"has invalid register key {register!r}."
            )
        if not isinstance(block, dict):
            _related_documents_invalid(
                f"related_documents `{register}`",
                f"must be a register table, got {type(block).__name__}.",
            )
        block = cast("dict[str, object]", block)
        unknown = set(block) - {"document"}
        if unknown:
            _related_documents_invalid(
                f"related_documents `{register}`",
                f"has unknown field(s): {', '.join(sorted(unknown))}.",
            )
        if "document" not in block:
            _related_documents_invalid(
                f"related_documents `{register}`",
                "needs at least one `[[register.<slug>.document]]` entry.",
            )
        entries = block["document"]
        if not isinstance(entries, list):
            _related_documents_invalid(
                f"related_documents `{register}`", "needs `document` table entries."
            )
        docs: list[RelatedDocument] = []
        for index, entry in enumerate(entries, start=1):
            if not isinstance(entry, dict):
                _related_documents_invalid(
                    f"related_documents `{register}` entry #{index}",
                    f"must be a table, got {type(entry).__name__}.",
                )
            doc = _parse_related_document(
                register, index, cast("dict[str, object]", entry)
            )
            key = (register, doc.filename)
            if key in seen:
                _related_documents_invalid(
                    f"related_documents `{register}` entry #{index}",
                    f"duplicates filename {doc.filename!r}.",
                )
            seen.add(key)
            docs.append(doc)
        out[register] = docs
    return out


def _register_dirs(path: Path | None) -> set[str]:
    if path is None or not path.is_dir():
        return set()
    return {child.name for child in path.iterdir() if child.is_dir()}


def _insert_related_documents(
    conn: sqlite3.Connection,
    related_documents: dict[str, list[RelatedDocument]],
    *,
    docs_dir: Path,
    related_docs_dir: Path | None,
) -> int:
    active_registers = _register_dirs(docs_dir) | _register_dirs(related_docs_dir)
    if not active_registers:
        return 0

    mapped_files = {
        (register, doc.filename)
        for register, docs in related_documents.items()
        for doc in docs
        if register in active_registers
    }
    unmapped_files: list[str] = []
    if related_docs_dir is not None and related_docs_dir.is_dir():
        for register_dir in sorted(
            child for child in related_docs_dir.iterdir() if child.is_dir()
        ):
            for file in sorted(register_dir.iterdir()):
                if (
                    file.is_file()
                    and file.suffix.lower() == ".pdf"
                    and (register_dir.name, file.name) not in mapped_files
                ):
                    unmapped_files.append(f"{register_dir.name}/{file.name}")
    if unmapped_files:
        log.warning(
            "related document binaries with no curated entry in %s: %s",
            RELATED_DOCUMENTS_FILE,
            ", ".join(unmapped_files),
        )

    if not related_documents:
        return 0
    if related_docs_dir is None:
        skipped = [
            f"{register}/{doc.filename}"
            for register, docs in sorted(related_documents.items())
            for doc in docs
        ]
        if skipped:
            log.warning(
                "related document binary root input_data/SCB/docs is missing; "
                "skipping mapped related documents: %s",
                ", ".join(skipped),
            )

    missing_binaries: list[str] = []
    total = 0
    for register, docs in sorted(related_documents.items()):
        if register not in active_registers:
            continue
        for doc in docs:
            binary_path = (
                related_docs_dir / register / doc.filename
                if related_docs_dir is not None
                else None
            )
            if binary_path is None or not binary_path.is_file():
                missing_binaries.append(f"{register}/{doc.filename}")
                continue
            content = binary_path.read_bytes()
            conn.execute(
                "INSERT INTO related_document ("
                "register, title, filename, source_url, license, fetched, "
                "sha256, byte_size, content"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    doc.register,
                    doc.title,
                    doc.filename,
                    doc.source_url,
                    doc.license,
                    doc.fetched,
                    sha256(content).hexdigest(),
                    len(content),
                    content,
                ),
            )
            total += 1

    if missing_binaries:
        log.warning(
            "related document map entries with no binary under input_data/SCB/docs: %s",
            ", ".join(missing_binaries),
        )
    return total


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
    related_documents = load_related_documents()
    related_docs_dir = repo_related_document_binaries_dir()
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
            if isinstance(raw_source, str):
                source = raw_source.removesuffix(".md")
                mapping = source_map.get(source)
                if mapping is None:
                    unmapped_sources.add(source)
            else:
                source = raw_source
                mapping = None

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

    related_total = _insert_related_documents(
        conn,
        related_documents,
        docs_dir=docs_dir,
        related_docs_dir=related_docs_dir,
    )

    # Populate FTS index
    conn.execute("INSERT INTO doc_fts(doc_fts) VALUES('rebuild')")

    # Store metadata
    for key, value in (
        ("schema_version", DOC_SCHEMA_VERSION),
        ("doc_count", str(total)),
        ("related_document_count", str(related_total)),
    ):
        conn.execute(
            "INSERT INTO doc_meta (key, value) VALUES (?, ?)",
            (key, value),
        )

    conn.commit()
    conn.close()
    log.info("Indexed %d docs from %s → %s", total, docs_dir, db_path)
    return db_path
