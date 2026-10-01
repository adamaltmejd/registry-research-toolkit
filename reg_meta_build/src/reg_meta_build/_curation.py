"""Shared helpers for the maintainer-edited curation TOML loaders.

The TOML scaffold and field helpers serve concept-group worklists, tags,
SCB errata, and curated relations. Register contracts use Pydantic models in
`curation_tree.py`; checked decisions compile in `curation_compile.py`.

Canonical integers reject coercions that would silently change native IDs.
Folded column keys are shared by source identity, curation bindings, and sibling
checks, so a curated column matches the same spelling variants as its source.
"""

from __future__ import annotations

import functools
import tomllib
import unicodedata
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from reg_meta.errors import EXIT_CONFIG, RegMetaError

from reg_meta_build._resolved_common import _require_trimmed

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable


class SentinelCode(BaseModel):
    """One curated per-classification sentinel code: an exact code string plus
    the short human meaning that justifies keeping the binding when the code
    is observed (e.g. a bulk/missing token the source emits for uncoded
    members). Codes are literal strings — `"00000"` never equals `"0"` and
    no pattern, prefix, or global waiver is recognized. Strict + forbid-extra
    so a misspelled key or a non-string code fails fast at the read boundary
    instead of silently never matching."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    code: str
    meaning: str

    _meaning = field_validator("meaning")(_require_trimmed)


def load_sentinel_codes(
    raw: object,
    *,
    classification: str,
    code: str,
) -> tuple[SentinelCode, ...]:
    """Validate a classification's raw `sentinel_codes` TOML list.

    The list holds `{code, meaning}` tables only — unknown keys, non-string
    codes, missing/empty meanings, and duplicate codes fail fast (EXIT_CONFIG)
    under the caller's `code`. `None` (key absent) means no sentinels."""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise curation_error(
            code,
            f"Classification {classification!r} `sentinel_codes` must be a list "
            f"of `{{code, meaning}}` tables, got {raw!r}.",
            'Write one `{code = "00000", meaning = "not applicable"}` '
            "table per sentinel code, or drop the key.",
        )
    sentinels: list[SentinelCode] = []
    for item in raw:
        try:
            sentinel = SentinelCode.model_validate(item)
        except ValidationError as exc:
            raise curation_error(
                code,
                f"Classification {classification!r} has an invalid "
                f"`sentinel_codes` entry {item!r}: {exc.errors(include_url=False)[0]['msg']}.",
                "Each entry needs exactly `code` (exact string) and `meaning` "
                "(non-empty string); no other keys, no patterns.",
            ) from exc
        if any(seen.code == sentinel.code for seen in sentinels):
            raise curation_error(
                code,
                f"Classification {classification!r} lists sentinel code "
                f"{sentinel.code!r} more than once.",
                "List each sentinel code once per classification.",
            )
        sentinels.append(sentinel)
    return tuple(sentinels)


_REPO_CURATION = Path(__file__).resolve().parent.parent.parent / "curation"


def repo_curation_dir() -> Path | None:
    """The checkout's ``curation/`` tree; wheels do not ship curation."""
    return _REPO_CURATION if _REPO_CURATION.is_dir() else None


def repo_curation_path(file_name: str) -> Path | None:
    """Return one catalog-overlay file from the repo's ``curation/`` directory.

    Wheels do not ship maintainer curation, so a missing file resolves to
    ``None`` just like the loaders' explicit missing-path convention.
    """
    candidate = _REPO_CURATION / file_name
    return candidate if candidate.is_file() else None


def repo_worklist_path(file_name: str) -> Path | None:
    """Return one maintained generator worklist file from the package root."""
    candidate = _REPO_CURATION.parent / "worklists" / file_name
    return candidate if candidate.is_file() else None


@functools.cache
def fold_column(s: str) -> str:
    """Shared column key: NFKD-decompose, strip non-ASCII, lowercase.

    `Kön` becomes `kon`, and `PersonNr` becomes `personnr`. Source identity,
    storage declarations, and curated column bindings use this same fold.
    Cache distinct spellings to avoid repeated Unicode normalization.
    """
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


_INTEGER_DATA_TYPES = frozenset(
    {"integer", "int", "bigint", "smallint", "tinyint", "bit"}
)
_DECIMAL_DATA_TYPES = frozenset(
    {"decimal", "numeric", "float", "real", "money", "smallmoney"}
)
_TEXT_DATA_TYPES = frozenset({"text", "char", "varchar", "nchar", "nvarchar", "ntext"})
_DATE_DATA_TYPES = frozenset(
    {"date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"}
)


def data_type_class(value: str) -> str | None:
    """Classify catalog and supported SQL type names without guessing from substrings."""
    kind = value.lower()
    if kind in _INTEGER_DATA_TYPES:
        return "integer"
    if kind in _DECIMAL_DATA_TYPES:
        return "decimal"
    if kind in _TEXT_DATA_TYPES:
        return "text"
    if kind in _DATE_DATA_TYPES:
        return "date"
    return None


def widen_data_type_classes(classes: Iterable[str]) -> str | None:
    """Widen type evidence; a date/numeric mixture has no safe common type."""
    kinds = set(classes)
    if "date" in kinds and kinds & {"integer", "decimal"}:
        return None
    if "text" in kinds:
        return "text"
    if "date" in kinds:
        return "date"
    if "decimal" in kinds:
        return "decimal"
    if "integer" in kinds:
        return "integer"
    return None


# data_type marker substrings. SCB ships SQL-ish lowercased types (`int`,
# `text`); SOS-style Swedish labels (`Heltal`, `Sträng (text)`, `Datum`) reach
# the same column. Substring match is safe — the field only ever holds a type
# name — and the two marker sets are disjoint across known types, so order
# doesn't matter. Hoisted here (with `_data_type_class` below) so source sibling checks
# (`source_siblings.py`) and the read-only split-sibling diagnostic
# (`split_sibling_suspects.py`) share ONE numeric/text/other classifier — the
# import-bug shape signal must not diverge between the build-time split and the
# diagnostic that re-derives it. `fold_column` (above) is the only dependency, so
# this leaf lives with it rather than in a provider-specific adapter.
_NUMERIC_TYPE_MARKERS = ("int", "tal", "num", "dec", "float", "real", "double")
_TEXT_TYPE_MARKERS = ("text", "char", "strang", "string", "varchar")


def _data_type_class(dt: str | None) -> str:
    """Coarse `numeric` / `text` / `other` class for a data_type. `other` covers
    dates and anything unrecognized (never claimed numeric or text). Folds via
    `fold_column` so `Sträng (text)` and `Heltal` classify by their ASCII form."""
    s = fold_column(dt) if dt else ""
    if any(m in s for m in _TEXT_TYPE_MARKERS):
        return "text"
    if any(m in s for m in _NUMERIC_TYPE_MARKERS):
        return "numeric"
    return "other"


# Code/label column-pair detection. Hoisted here (alongside `_data_type_class`)
# so source sibling checks (`source_siblings.py`) and the read-only diagnostic
# (`split_sibling_suspects.py`) apply ONE code-vs-label name heuristic — the build
# checks it BEFORE the import-bug shape heuristic (a `<stem>` code + its
# `<stem>namn` label is a representation pair, NOT a mis-typed delivery), so the
# diagnostic must apply the same precedence or it mislabels code/label pairs as
# `type_flip`. A label column carries the Swedish `namn` (name) suffix; its
# partner code column is either the bare stem (`Kommun`/`Kommunnamn`) or carries a
# `kod`/`id` code suffix (`Lid`/`LNamn`, `Sun2000Kod`/`Sun2000Namn`). `fold_column`
# (above) is the only dependency, so these leaves live with it.
_CODE_SUFFIXES = ("kod", "id")
_LABEL_SUFFIX = "namn"


def _strip_suffix(folded: str, suffixes: tuple[str, ...]) -> str | None:
    """The non-empty stem when `folded` ends with one of `suffixes`, else None."""
    for suf in suffixes:
        if folded.endswith(suf) and len(folded) > len(suf):
            return folded[: -len(suf)]
    return None


def _is_code_then_label(code: str, label: str) -> bool:
    """True when `code` is a code column and `label` its matching label column:
    `label` is `<stem>namn` and `code` is either the bare `<stem>` or
    `<stem>kod`/`<stem>id` on the SAME stem. Both args are already `fold_column`-
    folded."""
    stem = _strip_suffix(label, (_LABEL_SUFFIX,))
    if stem is None:
        return False
    if code == stem:  # bare-stem code paired with its `<stem>namn` label
        return True
    return _strip_suffix(code, _CODE_SUFFIXES) == stem


def _looks_like_code_label_pair(col_a: str, col_b: str) -> bool:
    """A code column paired with its label column, in either order. Name-based
    only (the old #132 heuristic, re-derived to current conventions). Folds each
    column via the shared `fold_column` key."""
    a, b = fold_column(col_a), fold_column(col_b)
    if not a or not b:
        return False
    return _is_code_then_label(a, b) or _is_code_then_label(b, a)


def sibling_shape_conflict(
    a: tuple[str | None, str | None], b: tuple[str | None, str | None]
) -> bool:
    """Existing split-group guard: numeric/text mismatch, else unknown-type width.

    Different lengths within a known type class do not establish a conflict.
    """
    class_a, class_b = _data_type_class(a[0]), _data_type_class(b[0])
    if {class_a, class_b} == {"numeric", "text"}:
        return True
    return bool("other" in (class_a, class_b) and a[1] and b[1] and a[1] != b[1])


def canonical_int(value: object) -> int | None:
    """Coerce a TOML `register_id` / `var_id` value to its canonical int, or None
    if it isn't one. A TOML integer is already canonical (the format forbids
    leading zeros); a string is accepted only in canonical form — no leading
    zeros, so `"01"` can't alias `1` (mirrors fqid_slugs `_parse_canonical_int`).
    A bool (TOML true/false, a Python int subclass) and a float are rejected."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str):
        if not value or not value.isdigit():
            return None
        if len(value) > 1 and value[0] == "0":
            return None
        return int(value)
    return None


def curation_error(code: str, message: str, remediation: str) -> RegMetaError:
    """A configuration-class error (EXIT_CONFIG) for the maintainer-edited
    curation TOMLs. A syntax typo or a malformed/dangling entry is a config
    failure with actionable remediation — not an internal build bug (which is
    how a raw tomllib/ValueError would surface through the CLI's generic
    handler). Single factory so every curation surface reports identically."""
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code=code,
        error_class="configuration",
        message=message,
        remediation=remediation,
    )


def load_curation_entries(
    path: Path | None,
    *,
    entry_key: str,
    label: str,
    prefix: str,
    code_base: str,
    file_name: str,
    entry_fields: str,
    sibling_keys: frozenset[str] = frozenset(),
) -> list[dict]:
    """The shared load scaffold for the curation TOMLs: read + parse, strict
    top-level-key guard (a misspelled ``[[{entry_key}s]]`` is a loud error, not
    a silent no-op that disables ALL curation), array-of-tables check, and
    per-entry table check. Returns the raw entry dicts — per-entry FIELD
    validation stays in each loader (their schemas differ).

    ``sibling_keys`` lists OTHER legal top-level keys in the same file (a file
    that carries more than one entry type, e.g. ``curation/registers/<provider>/<slug>.toml``'s
    ``[[description]]`` + ``[[alias]]``): they are not flagged as unknown, and
    each is loaded by its own call. ``[]`` when ``path`` is None/missing
    (synthetic test builds, wheel installs). Errors carry
    ``{code_base}_toml_unreadable`` / ``{code_base}_invalid`` so each surface
    keeps its established codes."""
    if path is None or not path.is_file():
        return []
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise curation_error(
            f"{code_base}_toml_unreadable",
            f"Could not parse {label} curation TOML {path}: {exc}",
            f"Fix the TOML syntax in reg_meta_build/{file_name}.",
        ) from exc
    unknown_top = set(data) - {entry_key} - sibling_keys
    if unknown_top:
        raise curation_error(
            f"{code_base}_invalid",
            f"{prefix} TOML has unknown top-level key(s): {sorted(unknown_top)}.",
            f"The only legal table is `[[{entry_key}]]` — check for a typo like "
            f"`[[{entry_key}s]]` in reg_meta_build/{file_name}.",
        )
    entries = data.get(entry_key, [])
    if not isinstance(entries, list):
        raise curation_error(
            f"{code_base}_invalid",
            f"{prefix} `{entry_key}` must be an array of tables "
            f"(`[[{entry_key}]]`), got {type(entries).__name__}.",
            f"Use `[[{entry_key}]]` table entries in reg_meta_build/{file_name}, "
            f"not `{entry_key} = …` or a single `[{entry_key}]` table.",
        )
    for entry in entries:
        if not isinstance(entry, dict):
            raise curation_error(
                f"{code_base}_invalid",
                f"{prefix} entry {entry!r} must be a `[[{entry_key}]]` table.",
                f"Each entry is a `[[{entry_key}]]` table with {entry_fields}.",
            )
    return entries


# ── per-entry leaf helpers ──────────────────────────────────────────────────
# Each loader binds the per-loader `code` / `prefix` / `file_name` once (a
# module-level `functools.partial`) so its established codes/messages are kept
# and its call sites stay unchanged.


def require_str(
    entry: dict,
    field: str,
    context: str,
    *,
    code: str,
    prefix: str,
    file_name: str,
) -> str:
    """Require `entry[field]` to be a non-empty, non-whitespace string; return it
    stripped. A missing/blank required field is curation drift, not a silent
    default, so it raises an actionable `{code}` config error. (Validation
    standardizes on `.strip()` + whitespace-only rejection across every loader.)"""
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        raise curation_error(
            code,
            f"{prefix} {context} needs `{field}` as a non-empty string, got {value!r}.",
            f'Give `{field} = "<value>"` in reg_meta_build/{file_name}.',
        )
    return value.strip()


def require_evidence(
    entry: dict,
    context: str,
    *,
    code: str,
    prefix: str,
    file_name: str,
) -> str:
    """Require correction ``evidence`` plus a canonical ``noted`` date.

    The evidence is returned for the row-level provenance carrier; ``noted`` is
    curation-log metadata. Keeping the date parser here gives every correction
    surface the same strict YYYY-MM-DD rule.
    """
    evidence = require_str(
        entry,
        "evidence",
        context,
        code=code,
        prefix=prefix,
        file_name=file_name,
    )
    noted = require_str(
        entry,
        "noted",
        context,
        code=code,
        prefix=prefix,
        file_name=file_name,
    )
    try:
        parsed = date.fromisoformat(noted)
    except ValueError:
        parsed = None
    if parsed is None or parsed.isoformat() != noted:
        raise curation_error(
            code,
            f"{prefix} {context} needs `noted` as YYYY-MM-DD, got {noted!r}.",
            'Use the date the correction was recorded, e.g. `noted = "2026-09-11"`.',
        )
    return evidence


def require_bool(
    entry: dict,
    field: str,
    context: str,
    *,
    code: str,
    prefix: str,
    file_name: str,
) -> bool:
    """Require an OPTIONAL `entry[field]` to be a real TOML boolean; absent → False
    (the DDL default). A present non-bool is rejected — `bool(...)` coercion is a
    footgun (`bool("false")` is True), and these fields back PII/identifier
    guardrails (`is_identifier` / `is_sensitive`), so a silently flipped flag is
    exactly the leak to prevent. The strict-bool semantics MUST stay byte-preserved
    across every loader that binds this leaf."""
    value = entry.get(field)
    if value is None:
        return False
    if not isinstance(value, bool):
        raise curation_error(
            code,
            f"{prefix} {context}: `{field}` must be a boolean when present, "
            f"got {value!r}.",
            f"Use a bare true/false for `{field}` (no quotes) in "
            f"reg_meta_build/{file_name}.",
        )
    return value


def require_fqid(
    entry: dict,
    field: str,
    *,
    code: str,
    prefix: str,
    entry_table: str,
    file_name: str,
    example: str = "scb/lisa/<variable>",
) -> tuple[str, str, str]:
    """Require `entry[field]` to be a 3-segment `provider/register/variable` FQID
    string; return the split `(provider, register, variable)`. A missing/malformed
    FQID is curation drift → `{code}` config error. `example` tailors the
    remediation FQID for the loader's domain (e.g. `scb/ulf/<variable>`)."""
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise curation_error(
            code,
            f"{prefix} {entry_table} needs `{field}` as a non-empty string, "
            f"got {value!r}.",
            f'Give `{field} = "{example}"`-style 3-segment FQIDs in '
            f"reg_meta_build/{file_name}.",
        )
    parts = value.split("/")
    if len(parts) != 3 or not all(parts):
        raise curation_error(
            code,
            f"{prefix} `{field}` {value!r} must be a 3-segment "
            "`provider/register/variable` FQID.",
            f'Give `{field} = "{example}"`-style 3-segment FQIDs.',
        )
    return (parts[0], parts[1], parts[2])


def resolve_variable_id(
    conn: sqlite3.Connection, provider: str, register: str, variable: str
) -> int | None:
    """`provider/register/variable` FQID → `variable_id`, or None if it doesn't
    resolve (pure lookup, no raise — each caller decides whether None is fatal)."""
    row = conn.execute(
        "SELECT v.variable_id FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
        (provider, register, variable),
    ).fetchone()
    return row[0] if row is not None else None


def resolve_register_id(
    conn: sqlite3.Connection, provider: str, register: str
) -> int | None:
    """`provider/register` FQID → `register_id`, or None if it doesn't resolve
    (pure lookup, no raise — each caller decides whether None is fatal)."""
    row = conn.execute(
        "SELECT r.register_id FROM register r "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND r.slug = ?",
        (provider, register),
    ).fetchone()
    return row[0] if row is not None else None
