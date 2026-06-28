"""Shared helpers for the maintainer-edited curation TOML loaders.

The scaffold (`load_curation_entries`, `curation_error`, `canonical_int`,
`fold_column`) plus the per-entry leaf helpers below (`require_str`,
`require_bool`, `require_fqid`, `resolve_variable_id`, `resolve_register_id`,
`resolve_register_variant_id`) serve the `[[entry]]`
curation-TOML loaders —
`codelivery.py`, `concept_groups.py`, `tags.py`,
`period_family_merges.py`, `delivery_enrichment.py`, `variable_grafts.py`,
`classification_links.py`, and `relations.py` (the single typed `[[edge]]`
surface for the curated pairwise relations — same_as / replaced_by,
#522). Each loader threads its own `code` / `prefix` / `file_name` through
(typically via a module-level `functools.partial`) so its established error
codes (and near-identical messages) are preserved.
The exceptions are `classifications.py` / `fqid_slugs.py` / `extend_db.py`, whose
data shapes differ enough that they don't share this scaffold.

Two single-definition invariants the helpers enforce: id keys MUST canonicalize
identically — a leniently coerced id (`int(1.5)`, `int(True)`, `int("01")`, a
negative) silently produces an inert never-matching curation pin instead of an
actionable load-time error (`canonical_int`); and the loaders' column keys must
fold EXACTLY like the SCB coalescer's union-find node-col (`sources/scb.py`
`_ascii_fold_lower` delegates here), or a curated column silently stops matching
its triage component (`fold_column`).
"""

from __future__ import annotations

import functools
import tomllib
import unicodedata
from typing import TYPE_CHECKING

from reg_meta.errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


@functools.cache
def fold_column(s: str) -> str:
    """Canonical column-identity key: NFKD-decompose, strip non-ASCII, lowercase
    (`Kön` → `kon`, `PersonNr` → `personnr`). This is the SCB rule-2 connectivity
    key — case/diacritic column twins fold to one union-find node — and therefore
    the form every curated column key is normalized to at load time. Cached: the
    coalescer folds per row-column over ~515K instance rows, but the domain is
    the corpus's distinct header spellings (tens of thousands), so a process-
    lifetime cache is small and saves repeated NFKD passes."""
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


# data_type marker substrings. SCB ships SQL-ish lowercased types (`int`,
# `text`); SOS-style Swedish labels (`Heltal`, `Sträng (text)`, `Datum`) reach
# the same column. Substring match is safe — the field only ever holds a type
# name — and the two marker sets are disjoint across known types, so order
# doesn't matter. Hoisted here (with `_data_type_class` below) so the SCB triage
# (`sources/scb.py`) and the read-only split-sibling diagnostic
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
    that carries more than one entry type, e.g. ``delivery_enrichment.toml``'s
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


def resolve_register_variant_id(
    conn: sqlite3.Connection, provider: str, register: str, variant: str
) -> tuple[int, int] | None:
    """`(provider, register, variant)` slugs → `(register_variant_id, register_id)`,
    or None if the variant doesn't resolve (pure lookup, no raise — each caller
    decides whether None is fatal). The shared target-resolution query for the
    post-passes that mint rows onto an EXISTING `(register, variant)` —
    `variable_grafts` and `canonical_attach`."""
    row = conn.execute(
        "SELECT rv.register_variant_id, r.register_id FROM register_variant rv "
        "JOIN register r ON r.register_id = rv.register_id "
        "JOIN provider p ON p.provider_id = r.provider_id "
        "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ?",
        (provider, register, variant),
    ).fetchone()
    return (row[0], row[1]) if row is not None else None
