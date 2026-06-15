"""Curated cross-register "see also" edges (#353).

A weak, maintainer-asserted association between two variables that are NOT the
same definition but are worth surfacing together when browsing (e.g. an scb
income concept and its sos welfare counterpart). The edges land in the existing
`variable_related_to` table (the same one the A2.2 triage feeds with
`auto:triage` split-sibling edges), keeping ONE relation surface — but on a
DISJOINT relation-kind vocabulary so a curated edge can never be mistaken for a
split sibling.

Two reasons the vocabularies MUST stay disjoint:
  - The concept-group edge pass (`concept_groups._derive_edge_groups`) folds the
    auto kind `same_definition_different_column` into one browse row. A curated
    "see also" is a WEAK link between distinct concepts — folding it would
    corrupt presentation identity. A curated kind that is foldable is a bug.
  - `note` distinguishes provenance (`auto:triage` vs. `curated`), but the
    relation_kind is the load-bearing signal consumers branch on; keeping the
    vocabularies disjoint means a consumer can trust the kind alone.

This is the loader machinery. The first curated edges landed in #403 — three
cross-register "see also" pairs. Like the other curation TOMLs
(`concept_groups.toml`, `fold_overrides.toml`) it is a maintainer artifact —
absent in wheel installs and synthetic test builds.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from ._curation import curation_error, load_curation_entries

# Curated relation-kind vocabulary. Grows with curation needs (add the kind here
# + document its meaning). MUST stay disjoint from the auto:triage kind
# `same_definition_different_column` — that kind is foldable by the concept-group
# edge pass, and a curated "see also" must never fold (see module docstring).
CURATED_RELATION_KINDS: frozenset[str] = frozenset({"similar_concept"})

# The auto:triage kind the concept-group edge pass folds on. Listed here only to
# assert (load-time + in tests) that no curated kind aliases it.
_AUTO_FOLDABLE_KIND = "same_definition_different_column"

# Default `note` for a curated edge that doesn't set one — provenance marker
# distinguishing these rows from the auto:triage edges in the same table.
_CURATED_NOTE_DEFAULT = "curated"


@dataclass(frozen=True)
class CuratedRelatedTo:
    """One `[[related]]` "see also" edge from `variable_related_to.toml`: an
    unordered pair of variable FQIDs (`a_*` / `b_*`, each a 3-segment
    provider/register/variable), the curated `relation_kind`, and an optional
    `note`. FQID resolution (both endpoints exist) happens at materialize time
    against the built DB, not at load."""

    a_provider: str
    a_register: str
    a_variable: str
    b_provider: str
    b_register: str
    b_variable: str
    relation_kind: str
    note: str | None


def repo_variable_related_to_path() -> Path | None:
    """`reg_meta_build/variable_related_to.toml` from a repo checkout, or None
    (wheel installs don't ship curation — it's a maintainer artifact like the
    slug TOMLs). Sits at the package root, NOT under `fqid_slugs/` (that dir is
    glob-loaded as provider-slug TOMLs; a file there would break the build)."""
    candidate = (
        Path(__file__).resolve().parent.parent.parent / "variable_related_to.toml"
    )
    return candidate if candidate.is_file() else None


def _require_fqid(entry: dict, field: str) -> tuple[str, str, str]:
    """Parse a `provider/register/variable` 3-segment FQID string (mirrors
    concept_groups' inline 2-segment split)."""
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise curation_error(
            "variable_related_to_invalid",
            f"variable_related_to [[related]] needs `{field}` as a non-empty "
            f"string, got {value!r}.",
            f'Give `{field} = "scb/lisa/<variable>"`-style 3-segment FQIDs in '
            "reg_meta_build/variable_related_to.toml.",
        )
    parts = value.split("/")
    if len(parts) != 3 or not all(parts):
        raise curation_error(
            "variable_related_to_invalid",
            f"variable_related_to {field} {value!r} must be a 3-segment "
            "`provider/register/variable` FQID.",
            'Give `a = "scb/lisa/<variable>"`-style 3-segment FQIDs.',
        )
    return (parts[0], parts[1], parts[2])


def load_related_to(path: Path | None) -> tuple[CuratedRelatedTo, ...]:
    """Parse the curated "see also" TOML. Empty when no file (synthetic test
    builds, wheel installs) or no entries.

    Load-time validation (all EXIT_CONFIG, actionable): only `[[related]]`
    top-level; `a`/`b` are 3-segment `provider/register/variable` FQID strings;
    `relation_kind` is a non-empty string ∈ `CURATED_RELATION_KINDS` (rejecting
    the auto kind is how distinctness from auto:triage is guaranteed); no
    self-edge (a == b); no duplicate UNORDERED FQID pair within the file; `note`
    optional but non-empty if present.

    Endpoint RESOLUTION (both variables exist in the built DB) happens at
    materialize time, not here — the same load/resolve split as
    `concept_groups` (structure + vocabulary are statically checkable; FQID
    existence is not)."""
    # Shared scaffold (parse + top-level typo guard + array-of-tables +
    # per-entry table check) — see `_curation.load_curation_entries`.
    entries = load_curation_entries(
        path,
        entry_key="related",
        label="variable-related-to",
        prefix="variable_related_to",
        code_base="variable_related_to",
        file_name="variable_related_to.toml",
        entry_fields="a / b / relation_kind",
    )
    out: list[CuratedRelatedTo] = []
    # Unordered FQID pairs already seen — frozenset of the two 3-tuples, so the
    # same pair in either a/b order collides. A duplicate is curation drift, not
    # something to silently dedup.
    seen_pairs: set[frozenset[tuple[str, str, str]]] = set()
    for entry in entries:
        a = _require_fqid(entry, "a")
        b = _require_fqid(entry, "b")
        if a == b:
            raise curation_error(
                "variable_related_to_invalid",
                f"variable_related_to entry relates {'/'.join(a)} to itself.",
                "A see-also edge connects two DISTINCT variables; remove the "
                "self-edge.",
            )
        kind = entry.get("relation_kind")
        if not isinstance(kind, str) or not kind:
            raise curation_error(
                "variable_related_to_invalid",
                f"variable_related_to entry {entry!r} needs `relation_kind` as a "
                f"non-empty string, got {kind!r}.",
                f'Use `relation_kind = "<kind>"` with a kind in '
                f"{sorted(CURATED_RELATION_KINDS)}.",
            )
        if kind not in CURATED_RELATION_KINDS:
            raise curation_error(
                "variable_related_to_invalid",
                f"variable_related_to relation_kind {kind!r} is not a curated "
                f"kind {sorted(CURATED_RELATION_KINDS)} (the auto:triage kind "
                f"{_AUTO_FOLDABLE_KIND!r} is foldable and is rejected here).",
                "Use a curated relation_kind, or add the new kind to "
                "CURATED_RELATION_KINDS in reg_meta_build/variable_related_to.py.",
            )
        pair = frozenset({a, b})
        if pair in seen_pairs:
            raise curation_error(
                "variable_related_to_invalid",
                f"variable_related_to has a duplicate unordered pair "
                f"{{{'/'.join(a)}, {'/'.join(b)}}}.",
                "List each variable pair once (the edge is symmetric — a→b and "
                "b→a are the same pair).",
            )
        seen_pairs.add(pair)
        note = entry.get("note")
        if note is not None and (not isinstance(note, str) or not note):
            raise curation_error(
                "variable_related_to_invalid",
                f"variable_related_to entry {entry!r} `note` must be a non-empty "
                f"string when present, got {note!r}.",
                "Drop `note` or give it a non-empty value like `note = "
                '"curated:cross_register"`.',
            )
        out.append(
            CuratedRelatedTo(
                a_provider=a[0],
                a_register=a[1],
                a_variable=a[2],
                b_provider=b[0],
                b_register=b[1],
                b_variable=b[2],
                relation_kind=kind,
                note=note,
            )
        )
    return tuple(out)


def _resolve_variable(
    conn: sqlite3.Connection, provider: str, register: str, variable: str
) -> bool:
    """True iff `provider/register/variable` resolves to a real variable (the
    same provider.slug/register.slug/variable.slug join the auto materializer
    uses, run in reverse: FQID → existence)."""
    row = conn.execute(
        "SELECT 1 FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
        (provider, register, variable),
    ).fetchone()
    return row is not None


def materialize_curated_related_to(
    conn: sqlite3.Connection,
    entries: tuple[CuratedRelatedTo, ...],
    *,
    providers: frozenset[str] = frozenset(),
) -> int:
    """Write curated "see also" edges (both directions) into
    `variable_related_to`. Mirrors `_materialize_variable_related_to` but
    resolves FQID → variable existence (the auto pass resolves variable_id →
    FQID); structural + vocabulary validation already happened at load
    (`load_related_to`), so this pass only resolves against the built DB — the
    same load/resolve split as `concept_groups`. Returns the row count inserted
    (both directions counted, like the auto materializer).

    `providers` gates each edge to the providers in this build (mirrors
    `materialize_concept_groups`): an edge whose `a_provider` or `b_provider`
    isn't built is SKIPPED, not failed — a `--providers=sos` build genuinely
    cannot represent an scb endpoint, so that's deferral, not curation drift.
    An edge whose providers ARE built but whose variable doesn't resolve IS
    drift → fail fast (EXIT_CONFIG)."""
    n_inserted = 0
    for e in entries:
        if e.a_provider not in providers or e.b_provider not in providers:
            continue
        a_fqid = f"{e.a_provider}/{e.a_register}/{e.a_variable}"
        b_fqid = f"{e.b_provider}/{e.b_register}/{e.b_variable}"
        if not _resolve_variable(conn, e.a_provider, e.a_register, e.a_variable):
            raise curation_error(
                "variable_related_to_unresolved",
                f"variable_related_to edge endpoint {a_fqid!r} does not resolve "
                "to a variable.",
                "Fix the `a` FQID in reg_meta_build/variable_related_to.toml.",
            )
        if not _resolve_variable(conn, e.b_provider, e.b_register, e.b_variable):
            raise curation_error(
                "variable_related_to_unresolved",
                f"variable_related_to edge endpoint {b_fqid!r} does not resolve "
                "to a variable.",
                "Fix the `b` FQID in reg_meta_build/variable_related_to.toml.",
            )
        note = e.note if e.note is not None else _CURATED_NOTE_DEFAULT
        # Plain INSERT (NOT OR IGNORE): a PK collision with an already-present
        # edge (an auto:triage sibling within a register, or another curated
        # edge) is curation drift, not a benign re-add — fail loud rather than
        # silently drop the curated kind/note.
        try:
            conn.executemany(
                "INSERT INTO variable_related_to "
                "(a_provider, a_register, a_variable, b_provider, b_register, "
                " b_variable, relation_kind, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        e.a_provider,
                        e.a_register,
                        e.a_variable,
                        e.b_provider,
                        e.b_register,
                        e.b_variable,
                        e.relation_kind,
                        note,
                    ),
                    (
                        e.b_provider,
                        e.b_register,
                        e.b_variable,
                        e.a_provider,
                        e.a_register,
                        e.a_variable,
                        e.relation_kind,
                        note,
                    ),
                ],
            )
        except sqlite3.IntegrityError as exc:
            raise curation_error(
                "variable_related_to_collision",
                f"variable_related_to curated edge {{{a_fqid}, {b_fqid}}} collides "
                "with an edge already present (auto:triage sibling or another "
                "curated edge).",
                "Remove the duplicate edge from "
                "reg_meta_build/variable_related_to.toml.",
            ) from exc
        n_inserted += 2
    return n_inserted
