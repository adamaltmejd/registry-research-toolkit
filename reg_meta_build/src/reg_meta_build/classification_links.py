"""Curated variable → classification links (#416 tail).

The build-time `link_value_set_classifications` detector
(`classifications.py`) auto-links the CONFIDENT tier — value sets whose codes
unambiguously enumerate one classification. The residue is the family-ambiguous
short numeric sets (SNI/SSYK/SUN coincide below ~15 codes) the detector
deliberately leaves for a human. This is the curated override for that tail: a
maintainer names `variable → classification` directly, and it takes precedence
over every auto/feed candidate.

Like the other curation TOMLs (`curation/relations.toml`, `concept_groups.toml`)
it is a maintainer artifact — absent in wheel installs and synthetic test builds —
and uses the same load/resolve split: structure + vocabulary are checked at load,
FQID / short_name existence at materialize time against the built DB.

The file SHIPS EMPTY today (just a documented format header). The residue
curation itself is a deferred follow-up; the loader handles zero entries cleanly.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ._curation import (
    curation_error,
    load_curation_entries,
    require_fqid,
    resolve_variable_id,
)

if TYPE_CHECKING:
    import sqlite3


@dataclass(frozen=True)
class CuratedClassificationLink:
    """One `[[link]]` entry: a 3-segment `provider/register/variable` FQID, the
    target classification `short_name`, and an optional `note`. Resolution (the
    variable and the classification both exist in the built DB) happens at
    materialize time, not at load."""

    provider: str
    register: str
    variable: str
    classification: str
    note: str | None


def repo_classification_links_path() -> Path | None:
    """`reg_meta_build/classification_links.toml` from a repo checkout, or None
    (wheel installs don't ship curation — it's a maintainer artifact like the slug
    TOMLs). Sits at the package root, NOT under `fqid_slugs/` (that dir is
    glob-loaded as provider-slug TOMLs; a file there would break the build)."""
    candidate = (
        Path(__file__).resolve().parent.parent.parent / "classification_links.toml"
    )
    return candidate if candidate.is_file() else None


_require_fqid = functools.partial(
    require_fqid,
    code="classification_links_invalid",
    prefix="classification_links",
    entry_table="[[link]]",
    file_name="classification_links.toml",
    example="scb/ulf/<variable>",
)


def load_classification_links(
    path: Path | None,
) -> tuple[CuratedClassificationLink, ...]:
    """Parse the curated classification-link TOML. Empty when no file (synthetic
    test builds, wheel installs) or no entries.

    Load-time validation (all EXIT_CONFIG, actionable): only `[[link]]` top-level;
    `variable` is a 3-segment `provider/register/variable` FQID string;
    `classification` is a non-empty string (its existence is checked at
    materialize); no duplicate `variable` within the file; `note` optional but
    non-empty if present. The variable → classification RESOLUTION happens at
    materialize time, not here — the same load/resolve split as
    `variable_related_to` / `concept_groups`."""
    entries = load_curation_entries(
        path,
        entry_key="link",
        label="classification-links",
        prefix="classification_links",
        code_base="classification_links",
        file_name="classification_links.toml",
        entry_fields="variable / classification",
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


def materialize_classification_links(
    conn: sqlite3.Connection,
    entries: tuple[CuratedClassificationLink, ...],
    *,
    providers: frozenset[str] = frozenset(),
) -> int:
    """Write curated `variable → classification` links into the provider-blind
    `classification_candidate` table with PRECEDENCE: for each of the variable's
    `(variable_id, value_set_id)` state keys, DELETE any existing candidate rows
    then INSERT the curated `(variable_id, value_set_id, cls_id)`. Curated wins
    over every auto/feed candidate (run this BEFORE the auto detector so its
    additive guard then skips the curated keys).

    `providers` gates each entry to this build's providers (mirrors
    `materialize_curated_related_to`): an entry whose provider isn't built is
    SKIPPED (a `--providers=sos` build genuinely can't represent an scb variable —
    deferral, not drift). An entry whose provider IS built but whose FQID or
    `classification` short_name doesn't resolve IS drift → fail fast (EXIT_CONFIG).

    Returns the number of candidate rows inserted."""
    n_inserted = 0
    for e in entries:
        if e.provider not in providers:
            continue
        fqid = f"{e.provider}/{e.register}/{e.variable}"
        variable_id = resolve_variable_id(conn, e.provider, e.register, e.variable)
        if variable_id is None:
            raise curation_error(
                "classification_links_unresolved",
                f"classification_links variable {fqid!r} does not resolve to a "
                "variable.",
                "Fix the `variable` FQID in reg_meta_build/classification_links.toml.",
            )
        cls_row = conn.execute(
            "SELECT id FROM classification WHERE short_name = ?",
            (e.classification,),
        ).fetchone()
        if cls_row is None:
            raise curation_error(
                "classification_links_unresolved",
                f"classification_links classification {e.classification!r} (for "
                f"{fqid!r}) is not a seeded classification short_name.",
                "Use a short_name present in classifications.toml, or remove the "
                "entry from reg_meta_build/classification_links.toml.",
            )
        cls_id = cls_row[0]

        # Apply to every value-set state key the variable carries. A code-less
        # state (value_set_id NULL) is intentionally NOT linked here — a curated
        # classification link is about the variable's inline code set, so it
        # targets the value-set-bearing states. DELETE-then-INSERT gives curated
        # precedence over any auto/feed candidate already present for the key.
        state_keys = conn.execute(
            "SELECT DISTINCT value_set_id FROM variable_state "
            "WHERE variable_id = ? AND value_set_id IS NOT NULL",
            (variable_id,),
        ).fetchall()
        for (value_set_id,) in state_keys:
            conn.execute(
                "DELETE FROM classification_candidate "
                "WHERE variable_id = ? AND value_set_id IS ?",
                (variable_id, value_set_id),
            )
            conn.execute(
                "INSERT INTO classification_candidate "
                "(variable_id, value_set_id, classification_id) VALUES (?, ?, ?)",
                (variable_id, value_set_id, cls_id),
            )
            n_inserted += 1
    return n_inserted
