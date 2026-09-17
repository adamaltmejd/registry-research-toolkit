"""Parse accepted variable-to-classification declarations.

The loader validates their shape and vocabulary. The common pipeline resolves
checked source-bound decisions before materialization; this module never patches
an already built database.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._curation import (
    curation_error,
    load_curation_entries,
    require_fqid,
)

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class CuratedClassificationLink:
    """One `[[link]]` entry: a 3-segment `provider/register/variable` FQID, the
    target classification `short_name`, and an optional `note`. Source-bound
    resolution belongs to the common curation stage, not this parser."""

    provider: str
    register: str
    variable: str
    classification: str
    note: str | None


_require_fqid = functools.partial(
    require_fqid,
    code="classification_links_invalid",
    prefix="classification_links",
    entry_table="[[link]]",
    file_name="curation/classifications.toml",
    example="scb/ulf/<variable>",
)


def load_classification_links(
    path: Path | None,
) -> tuple[CuratedClassificationLink, ...]:
    """Parse the curated classification-link TOML. Empty when no file (synthetic
    test builds, wheel installs) or no entries.

    Load-time validation (all EXIT_CONFIG, actionable): only `[[link]]` top-level;
    `variable` is a 3-segment `provider/register/variable` FQID string;
    `classification` is a non-empty string; no duplicate `variable` within the
    file; `note` optional but non-empty if present. Catalog identity and source
    applicability are checked separately in the common curation stage."""
    entries = load_curation_entries(
        path,
        entry_key="link",
        label="classification-links",
        prefix="classification_links",
        code_base="classification_links",
        file_name="curation/classifications.toml",
        entry_fields="variable / classification",
        sibling_keys=frozenset({"classification"}),
    )
    out: list[CuratedClassificationLink] = []
    # A duplicate `variable` is curation drift (two rows would fight over the same
    # state keys), not something to silently dedup.
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        fqid = _require_fqid(entry, "variable")
        classification = entry.get("classification")
        if not isinstance(classification, str) or not classification:
            raise curation_error(
                "classification_links_invalid",
                f"classification_links entry {entry!r} needs `classification` as a "
                f"non-empty short_name string, got {classification!r}.",
                'Use `classification = "ICD-10-SE"` (a seeded short_name).',
            )
        if fqid in seen:
            raise curation_error(
                "classification_links_invalid",
                f"classification_links has a duplicate `variable` {'/'.join(fqid)}.",
                "List each variable once; a variable links to exactly one "
                "classification here.",
            )
        seen.add(fqid)
        note = entry.get("note")
        if note is not None and (not isinstance(note, str) or not note):
            raise curation_error(
                "classification_links_invalid",
                f"classification_links entry {entry!r} `note` must be a non-empty "
                f"string when present, got {note!r}.",
                "Drop `note` or give it a non-empty value.",
            )
        out.append(
            CuratedClassificationLink(
                provider=fqid[0],
                register=fqid[1],
                variable=fqid[2],
                classification=classification,
                note=note,
            )
        )
    return tuple(out)
