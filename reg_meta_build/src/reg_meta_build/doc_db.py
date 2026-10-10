"""Build pipeline for the reg_meta doc index.

Parses frontmatter + markdown bodies under a curated docs directory and
writes the FTS5-indexed `reg_meta_docs.db`. This module also owns the docs
schema constants, connection management and the schema-compat check.
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

from ._curation import curation_error
from .derive.search_index import register_fold_search
from .errors import EXIT_CONFIG, RegMetaError

DOC_DB_FILENAME = "reg_meta_docs.db"
DOC_SCHEMA_VERSION = "1.3.0"


def doc_db_path(db_arg: str | None) -> Path:
    """Resolve path to the doc index DB."""
    from .db import db_path_from_args

    return db_path_from_args(db_arg, filename=DOC_DB_FILENAME)


def _check_doc_schema_compat(conn: sqlite3.Connection, db_path: Path) -> None:
    """Raise if the doc DB schema is incompatible with the installed code.

    Mirrors ``_check_schema_compat`` in ``db.py``: same-major / minor>=code
    rule against ``DOC_SCHEMA_VERSION``. Missing/unparseable metadata is
    treated as incompatible so stale pre-versioning DBs get replaced.
    """
    fix = (
        "Run `reg-meta update` to replace it with a compatible asset. "
        "(Doc DBs built by pre-0.7 reg_meta lack schema_version and are always "
        "reported as incompatible — the update will overwrite them.)"
    )

    try:
        row = conn.execute(
            "SELECT value FROM doc_meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="doc_schema_incompatible",
            error_class="configuration",
            message=(
                f"Doc DB metadata is missing or unreadable in {db_path}. "
                f"Expected doc schema v{DOC_SCHEMA_VERSION}."
            ),
            remediation=fix,
        ) from exc

    db_ver = row["value"] if row else None
    try:
        if not db_ver:
            raise ValueError("missing schema_version")
        db_parts = db_ver.split(".")
        db_major, db_minor = int(db_parts[0]), int(db_parts[1])
        code_parts = DOC_SCHEMA_VERSION.split(".")
        code_major, code_minor = int(code_parts[0]), int(code_parts[1])
    except (ValueError, IndexError) as exc:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="doc_schema_incompatible",
            error_class="configuration",
            message=(
                f"Doc DB schema version is missing or invalid in {db_path}: "
                f"{db_ver!r}. This version of reg_meta expects doc schema v{DOC_SCHEMA_VERSION}."
            ),
            remediation=fix,
        ) from exc

    if db_major != code_major or db_minor < code_minor:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="doc_schema_incompatible",
            error_class="configuration",
            message=(
                f"Doc DB schema v{db_ver} ({db_path}) is incompatible with this "
                f"version of reg_meta (expects doc schema v{DOC_SCHEMA_VERSION})."
            ),
            remediation=fix,
        )


def open_doc_db(db_path: Path, *, check_schema: bool = True) -> sqlite3.Connection:
    """Open the doc index DB read-only and verify schema compatibility."""
    if not db_path.exists():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="doc_db_not_found",
            error_class="configuration",
            message=f"Doc DB not found: {db_path}",
            remediation="Run `reg-meta update` to fetch the doc DB.",
        )
    # `immutable=1` (read-only path only): unlike the main catalog DB, the doc DB
    # is built in (and shipped as) DELETE journal mode, so the #283 sidecar crash
    # cannot occur here — applied for symmetry with `db.open_db` and to
    # future-proof against the builder ever switching journal modes. Same locking
    # trade-off and same safety argument (see db.open_db): the doc DB is only ever
    # replaced whole (an installed release asset or a new build's atomic rename),
    # never mutated in place under a reader.
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    if check_schema:
        try:
            _check_doc_schema_compat(conn, db_path)
        except RegMetaError:
            conn.close()
            raise
    return conn


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

# The index holds `fold_search` text (decision 16), so `fold_search` is the only fold
# and the tokenizer only splits. It reads its display text from `doc` (external
# content): `snippet()` maps the index's token positions onto the stored body, so
# snippets keep their case and diacritics. Footgun: FTS5's 'rebuild' re-tokenizes the
# unfolded `doc` columns and so unfolds the index; never run it here, `index_docs`
# refills the index instead. `PRAGMA integrity_check` and the default
# 'integrity-check' are safe; a strict 'integrity-check' (rank 1) compares the index
# with the unfolded columns and reports a false "malformed".
DOC_FTS_DDL = """\
DROP TABLE IF EXISTS doc_fts;
CREATE VIRTUAL TABLE doc_fts USING fts5(
    display_name, variable, body_clean,
    content='doc', content_rowid='doc_id',
    tokenize='unicode61 remove_diacritics 0'
);
INSERT INTO doc_fts(rowid, display_name, variable, body_clean)
    SELECT doc_id, fold_search(display_name), fold_search(variable),
           fold_search(body_clean)
    FROM doc ORDER BY doc_id;
"""


def index_docs(conn: sqlite3.Connection) -> None:
    """(Re)fill `doc_fts` with `fold_search` text of `doc` and stamp the doc schema
    and the docs generation.

    The generation is the SHA-256 of the schema version and every `doc` row in
    `doc_id` order: what `docs_search` selects and orders, so its cursors go stale
    when the documents change, even when the catalog beside them does not.

    The docs build ends with it; G1 runs it over a copy of a pinned docs database to
    make its candidate copy. Commits.
    """
    register_fold_search(conn)
    conn.executescript(DOC_FTS_DDL)
    digest = sha256(DOC_SCHEMA_VERSION.encode())
    for row in conn.execute("SELECT * FROM doc ORDER BY doc_id"):
        digest.update(json.dumps(list(row), ensure_ascii=False).encode() + b"\n")
    conn.executemany(
        "INSERT OR REPLACE INTO doc_meta (key, value) VALUES (?, ?)",
        [("schema_version", DOC_SCHEMA_VERSION), ("generation", digest.hexdigest())],
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Frontmatter parser (no PyYAML dependency)
# ---------------------------------------------------------------------------

_FM_DELIM = re.compile(r"^---\s*$")


def parse_frontmatter(text: str) -> tuple[dict[str, object], str]:
    """Parse YAML frontmatter from markdown text.

    Returns (metadata_dict, body) where body is the text after frontmatter.
    Only handles the subset we generate: scalar values, simple lists, the
    folded/literal block scalars panache reflows long scalars into
    (``key: >``/``>-``/``>+`` folded, ``key: |``/``|-``/``|+`` literal), and
    plain multi-line flow scalars (a ``key: value`` whose value wraps onto bare
    indented continuation lines with no ``>``/``|`` indicator), folded with
    single spaces like a YAML plain scalar.
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

    i = 1
    while i < end:
        line = lines[i]

        # List item: "  - value"
        if line.startswith("  - ") and current_key:
            if current_list is None:
                current_list = []
            current_list.append(line[4:].strip())
            i += 1
            continue

        # Save accumulated list
        if current_list is not None and current_key:
            meta[current_key] = current_list
            current_list = None

        # Key-value: "key: value"
        if ":" in line:
            key, _, val = line.partition(":")
            key_indent = len(line) - len(line.lstrip(" "))
            key = key.strip()
            val = val.strip()
            current_key = key

            fold = _block_scalar_style(val)
            if fold is not None:
                value, i = _read_block_scalar(lines, i + 1, end, key_indent, fold)
                meta[key] = value
                continue

            val = val.strip('"').strip("'")
            if val:
                # Plain multi-line flow scalar: panache may wrap a long value with
                # no quote-forcing chars as a bare indented continuation (no `>`/`|`
                # indicator). Fold those continuation lines into the value with
                # single spaces so it isn't silently truncated to the first line.
                if _is_plain_flow_continuation(lines, i + 1, end, key_indent):
                    cont, i = _read_block_scalar(
                        lines, i + 1, end, key_indent, "folded"
                    )
                    meta[key] = f"{val} {cont}".strip() if cont else val
                    continue
                meta[key] = val
            # If val is empty, next lines might be a list
        else:
            current_key = None
        i += 1

    if current_list is not None and current_key:
        meta[current_key] = current_list

    body = "\n".join(lines[end + 1 :]).lstrip("\n")
    return meta, body


def _block_scalar_style(val: str) -> str | None:
    """Return "folded"/"literal" if ``val`` is a block-scalar indicator, else None.

    Recognizes ``>``/``>-``/``>+`` (folded) and ``|``/``|-``/``|+`` (literal),
    i.e. the ``key:`` value being an indicator char plus an optional chomping
    modifier and nothing else.
    """
    if val in {">", ">-", ">+"}:
        return "folded"
    if val in {"|", "|-", "|+"}:
        return "literal"
    return None


def _is_plain_flow_continuation(
    lines: list[str], start: int, end: int, key_indent: int
) -> bool:
    """True if ``lines[start]`` begins a plain multi-line flow-scalar continuation.

    A continuation is a non-blank line indented MORE than ``key_indent`` that is
    not a ``  - `` list item — the shape panache emits when it wraps a long value
    with no quote-forcing characters (no ``>``/``|`` indicator). Distinguishing it
    from a nested-list opener (``key:`` with an empty value) is why we only look
    past a NON-empty first-line value at the call site.
    """
    if start >= end:
        return False
    line = lines[start]
    if not line.strip():
        return False
    if (len(line) - len(line.lstrip(" "))) <= key_indent:
        return False
    return not line.lstrip(" ").startswith("- ")


def _read_block_scalar(
    lines: list[str], start: int, end: int, key_indent: int, style: str
) -> tuple[str, int]:
    """Read a block-scalar body starting at ``lines[start]``.

    The body is the run of lines indented more than ``key_indent``; it ends at
    the first line dedented to ``key_indent`` or less. Returns the joined,
    trimmed value and the index of the first line past the body.
    """
    body: list[str] = []
    i = start
    while i < end:
        line = lines[i]
        if line.strip() and (len(line) - len(line.lstrip(" "))) <= key_indent:
            break
        body.append(line)
        i += 1

    # Strip the common leading indentation from non-blank body lines.
    indents = [len(line) - len(line.lstrip(" ")) for line in body if line.strip()]
    common = min(indents) if indents else 0
    stripped = [line[common:] if line.strip() else "" for line in body]

    joiner = " " if style == "folded" else "\n"
    # simplify: folds blank lines to single spaces (not YAML paragraph-break
    # newlines) and .strip() ignores `+` keep-chomping. Fine because panache emits
    # only single-paragraph `>-`/`|-`/plain values for these display_names. Revisit
    # if panache starts emitting multi-paragraph block scalars (blank line inside).
    return joiner.join(stripped).strip(), i


# ---------------------------------------------------------------------------
# Docs source dir (in-repo)
# ---------------------------------------------------------------------------


def repo_docs_dir() -> Path | None:
    """Return the in-repo source-markdown directory, for dev-time builds only.

    Runtime NEVER reads from this — users receive the prebuilt doc DB as a
    release asset. Only ``reg-meta-build build-docs``
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
    sha256: str
    byte_size: int
    required: bool


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
            "source_url, license, fetched, sha256, and byte_size in "
            "reg_meta_build/related_documents.toml. Use `required = false` only "
            "for intentionally staged future entries."
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
    expected = {
        "title",
        "filename",
        "source_url",
        "license",
        "fetched",
        "sha256",
        "byte_size",
        "required",
    }
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
        parsed_fetched = date.fromisoformat(fetched)
    except ValueError as exc:
        raise curation_error(
            code="related_documents_invalid",
            message=f"{subject} needs `fetched` as YYYY-MM-DD, got {fetched!r}.",
            remediation=(
                "Use the date the maintainer fetched the binary, for example "
                '`fetched = "2026-06-23"`.'
            ),
        ) from exc
    if parsed_fetched.isoformat() != fetched:
        raise curation_error(
            code="related_documents_invalid",
            message=f"{subject} needs `fetched` as YYYY-MM-DD, got {fetched!r}.",
            remediation=(
                "Use the date the maintainer fetched the binary, for example "
                '`fetched = "2026-06-23"`.'
            ),
        )

    return RelatedDocument(
        register=register,
        title=_require_related_document_str(entry, "title", subject),
        filename=filename,
        source_url=_require_related_document_str(entry, "source_url", subject),
        license=_parse_related_document_license(entry, subject),
        fetched=fetched,
        sha256=_parse_related_document_sha256(entry, subject),
        byte_size=_parse_related_document_byte_size(entry, subject),
        required=_parse_related_document_required(entry, subject),
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


def _parse_related_document_sha256(entry: dict[str, object], subject: str) -> str:
    digest = _require_related_document_str(entry, "sha256", subject)
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        _related_documents_invalid(
            subject,
            f"needs `sha256` as 64 lowercase hex characters, got {digest!r}.",
        )
    return digest


def _parse_related_document_byte_size(entry: dict[str, object], subject: str) -> int:
    value = entry.get("byte_size")
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        _related_documents_invalid(
            subject,
            f"needs `byte_size` as a non-negative integer, got {value!r}.",
        )
    return value


def _parse_related_document_required(entry: dict[str, object], subject: str) -> bool:
    value = entry.get("required", True)
    if not isinstance(value, bool):
        _related_documents_invalid(
            subject,
            f"needs `required` as a boolean when present, got {value!r}.",
        )
    return value


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
    active_registers = (
        _register_dirs(docs_dir)
        | _register_dirs(related_docs_dir)
        | {
            register
            for register, docs in related_documents.items()
            if any(doc.required for doc in docs)
        }
    )
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
            digest = sha256(content).hexdigest()
            byte_size = len(content)
            if digest != doc.sha256 or byte_size != doc.byte_size:
                raise curation_error(
                    code="related_documents_binary_mismatch",
                    message=(
                        f"related document binary {register}/{doc.filename} does not "
                        "match the tracked pins: "
                        f"sha256 {digest} (expected {doc.sha256}), "
                        f"byte_size {byte_size} (expected {doc.byte_size})."
                    ),
                    remediation=(
                        "Verify the gitignored PDF seed. If the replacement is "
                        "intentional and license-compatible, update `sha256`, "
                        "`byte_size`, and usually `fetched` in "
                        "reg_meta_build/related_documents.toml."
                    ),
                )
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
                    digest,
                    byte_size,
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
        if stripped.startswith(("|", "---")):
            continue
        # Skip image references
        if stripped.startswith(("![]", "Image ")):
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


def build_doc_db(
    docs_dir: Path,
    db_dir: Path,
    *,
    related_documents: dict[str, list[RelatedDocument]] | None = None,
    related_docs_dir: Path | None = None,
) -> Path:
    """Build the doc search index from markdown files.

    Scans docs_dir for register subdirectories (e.g. lisa/),
    parses frontmatter from each .md file, and populates the
    FTS5 index.

    Related documents come only from the arguments: the curated map
    (``load_related_documents``) and the directory holding their binaries.
    Both default to none, so a library build never reads the repository's
    untracked seed; the ``build-docs`` CLI passes the repository paths.

    Returns the path to the created DB.
    """
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / DOC_DB_FILENAME

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    conn.executescript(DOC_DDL)

    source_map = load_doc_sources()
    related_documents = related_documents or {}
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

    for key, value in (
        ("doc_count", str(total)),
        ("related_document_count", str(related_total)),
    ):
        conn.execute(
            "INSERT INTO doc_meta (key, value) VALUES (?, ?)",
            (key, value),
        )

    index_docs(conn)
    conn.close()
    log.info("Indexed %d docs from %s → %s", total, docs_dir, db_path)
    return db_path
