"""Shared helpers for the maintainer-edited curation TOML loaders
(`codelivery.py`, `fold_overrides.py`, `column_merges.py`). All key their entries
on SCB/SOS source ids (`register_id` / `var_id`) and MUST canonicalize them
identically — a leniently coerced id (`int(1.5)`, `int(True)`, `int("01")`, a
negative) silently produces an inert never-matching curation pin instead of an
actionable load-time error, so the canonicalization lives in one place all
import. The same single-definition rule applies to `fold_column`: the loaders'
column keys must fold EXACTLY like the SCB coalescer's union-find node-col
(`sources/scb.py` `_ascii_fold_lower` delegates here), or a curated column
silently stops matching its triage component.
"""

from __future__ import annotations

import functools
import tomllib
import unicodedata
from typing import TYPE_CHECKING

from reg_meta.errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
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
    handler). Single factory so every curation surface (codelivery,
    fold_overrides, column_merges, concept_groups) reports identically."""
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
