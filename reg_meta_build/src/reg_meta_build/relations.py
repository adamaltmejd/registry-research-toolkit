"""One typed curation surface for the curated pairwise-relation facts (#522).

Four maintainer-authored relation surfaces used to live apart — `same_as` (split
between `variable_same_as.toml` and an inline `same_as` field on the slug TOMLs),
`related_to` (`variable_related_to.toml`), and `replaced_by` (a top-level
`[[replaced_by]]` array inside the slug TOMLs). They are three *kinds* of the
same thing — a curated assertion about a pair of catalog entities — so they now
share ONE file (`reg_meta_build/curation/relations.toml`) as a single `[[edge]]`
array discriminated by `type`, and ONE loader + materializer (this module).

The three relation kinds are genuinely different relations, and the materializer
keeps every prior behavior verbatim (the DB output is byte-identical — gated by
dbdiff):

  - `same_as` — symmetric, transitive IDENTITY ("one concept, two FQIDs").
    RESOLVER-LOAD-BEARING: `Catalog.resolve` follows it transitively and the
    build cycle-checks the as-declared graph, so a wrong edge corrupts
    resolution. Variable grain (3-seg `provider/register/variable`) OR
    classification grain (2-seg `provider/classification_slug`) — never mixed.
    Lands in `variable_same_as` / `classification_same_as`, both directions.
  - `replaced_by` — directional SUCCESSION (predecessor superseded by successor).
    NOT identity: the definitions differ across eras. Register grain (2-seg) OR
    variable grain (3-seg). The predecessor MAY be dead (retired / renamed /
    cross-provider — the whole reason this exists alongside the
    `timeseries_event`-derived edges, which can express neither). Lands in
    `register_replaced_by` / `variable_replaced_by`, one direction, sharing the
    event pass's seen-PK sets so a curated edge dedups against an event one and
    the combined per-grain graph is cycle-checked.
  - `related_to` — weak "see also" DISCOVERY link between distinct concepts. Lands
    in `variable_related_to` (the same table the A2.2 triage feeds with the
    NON-FOLDABLE `auto:triage` split kinds — `code_vs_label_pair`,
    `import_bug_suspect`) but on a DISJOINT relation-kind vocabulary
    (`CURATED_RELATION_KINDS`) so a curated weak link can never be mistaken for a
    split sibling. The bulk `same_definition_different_column` siblings are NOT in
    this table at all (#591) — they feed the concept-group edge fold directly from
    the in-build sibling sets — so a curated kind matching them is rejected by the
    allowlist on principle.

The same-as candidate GENERATOR (`infer_same_as_candidates`, #508) stays in
`variable_same_as.py`; it reads structured signals off a built DB and RENDERS a
review worklist as `[[edge]] type = "same_as"` TOML *text* — the exact shape this
loader accepts — so a confirmed candidate copies across into
`curation/relations.toml` verbatim. The boundary is text, not symbols: the
generator imports nothing from here, and the round-trip is the TOML grammar.

Like the other curation TOMLs (`concept_groups.toml`, `codelivery.toml`) the file
is a maintainer artifact — absent in wheel installs and synthetic test builds.
"""

from __future__ import annotations

import functools
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta.fqid import Fqid, FqidError, FqidKind, parse as parse_fqid

from ._curation import (
    curation_error,
    load_curation_entries,
    require_fqid,
    resolve_register_id,
    resolve_variable_id,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

# ── relation kind vocabularies ──────────────────────────────────────────────

# The legal `type` discriminators. Surfaced in the unknown-type error so a typo
# is self-correcting.
_EDGE_TYPES: frozenset[str] = frozenset({"same_as", "replaced_by", "related_to"})

# Curated `related_to` relation-kind vocabulary. Grows with curation needs (add
# the kind here + document its meaning). The allowlist is the gate: a curated
# `related_to` must name one of these kinds, so a typo or the bulk auto:triage
# split kind is rejected (the concept-group edge fold owns the bulk siblings, and
# a curated "see also" must never be one).
CURATED_RELATION_KINDS: frozenset[str] = frozenset({"similar_concept"})

# Default `note` for a curated related_to edge that doesn't set one — provenance
# marker distinguishing these rows from the auto:triage edges in the same table.
_CURATED_RELATED_NOTE_DEFAULT = "curated"

# Provenance marker for curated replaced_by edges (mirrors db.py so a consumer can
# tell curated from auto-derived). It lands in `note` for ALL three grains. For
# register/variable a row's own `note` (the human transition reason) lands in
# `beskrivning` beside it; the classification table has NO `beskrivning`, so a
# classification edge carries no transition reason at all — `note` is
# provenance-only and any human reason belongs in a `#` comment in relations.toml.
_REPLACED_BY_NOTE_CURATED = "curated:slug_toml"

# Provenance marker for the #584 derived variable vintage-succession edges:
# variable A → B lifted from a `classification_replaced_by` edition edge through
# the value-set bindings. Distinct from `auto:timeseries_event` (event-derived) /
# `curated:slug_toml` (hand-curated) so a consumer can tell the lift apart.
_REPLACED_BY_NOTE_VINTAGE_LIFT = "derived:classification_vintage_lift"

# same_as component-size guard (#522). A same_as edge MERGES two identity
# components into one; a single mistaken curated edge can therefore silently weld
# two large, genuinely-distinct concept clusters into one resolver blob. Refuse
# any edge whose merged component would exceed this many distinct FQIDs — a
# curated identity cluster that large is almost certainly a curation error, not a
# real concept. This now governs SHIPPED data: the #508 tier-1 batch carries 615
# edges across 62 identity components (max component 13 FQIDs), so the guard
# actively bounds the live data (with comfortable headroom under the cap).
_SAME_AS_MAX_COMPONENT = 32

# Replaced_by grains: register-, variable-, or classification-grain. The variant
# grain is deliberately out of scope — a variant is a delivery coordinate, not a
# curation surface for cross-provider succession. Classification grain (#579) is
# the `class/<slug>` form (a 1→many edition split the #571 auto rule can't
# produce, e.g. sun1996 → sun2000-niva + sun2000-inriktning).
_REPLACED_BY_GRAINS: frozenset[FqidKind] = frozenset(
    {FqidKind.REGISTER, FqidKind.VARIABLE_BINDING, FqidKind.CLASSIFICATION}
)

# Per-type accepted/foreign field maps (besides `type`). A field legal for one
# type is a FOREIGN key on another (e.g. `effective_year` on a same_as edge) and
# rejected — this catches a mis-typed edge (right fields, wrong `type`) at load.
_SAME_AS_FIELDS: frozenset[str] = frozenset({"a", "b", "note"})
# `from_column` / `to_column` (#843) ride a variable-grain replaced_by edge to
# name a REPRESENTATION endpoint `(variable_fqid, delivery_column)` — both or
# neither, both endpoints variable-grain. A column field on a same_as / related_to
# edge is foreign and rejected by their maps.
# `variant` (#846) optionally rides a REPRESENTATION edge (one carrying
# `from_column` / `to_column`) to scope the succession to a single
# register-variant: `''`/absent = variable-level (the default, whole-variable
# semantics), a variant slug = scoped. Legal ONLY on a representation edge and
# only WITH `effective_year` (the time-monotone cycle check needs the ordering) —
# both enforced in `_load_replaced_by`.
_REPLACED_BY_FIELDS: frozenset[str] = frozenset(
    {"from", "to", "effective_year", "note", "from_column", "to_column", "variant"}
)
_RELATED_TO_FIELDS: frozenset[str] = frozenset({"a", "b", "relation_kind", "note"})

_VarKey = tuple[str, str, str]
_ClassKey = tuple[str, str]


# ── dataclasses ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CuratedSameAs:
    """One `type = "same_as"` identity edge: an UNORDERED pair of FQIDs (`a` / `b`,
    same grain) plus an optional `note`. There is NO `relation_kind` — same_as
    carries no kind vocabulary; identity is identity.

    Both endpoints are either variable-grain (`a_provider/a_register/a_variable`,
    `b_*` the mirror) or classification-grain (`a_register`/`b_register` carry the
    classification slug, `a_variable`/`b_variable` are None). `grain` records
    which. Endpoint resolution happens at materialize time against the built DB,
    not at load; the variable/classification slug is NOT validated — same_as is
    slug-anchored and survives renames."""

    grain: FqidKind  # VARIABLE_BINDING or CLASSIFICATION
    a_provider: str
    a_register: str
    a_variable: str | None
    b_provider: str
    b_register: str
    b_variable: str | None
    note: str | None

    def a_fqid(self) -> str:
        return _join_fqid(self.a_provider, self.a_register, self.a_variable)

    def b_fqid(self) -> str:
        return _join_fqid(self.b_provider, self.b_register, self.b_variable)


@dataclass(frozen=True)
class CuratedReplacedBy:
    """One `type = "replaced_by"` succession edge, parsed FQID-shaped but
    DB-unverified. `predecessor` / `successor` (TOML `from` / `to`) are parsed
    `Fqid`s of the SAME grain — both register (2 segs), both variable (3 segs),
    or both classification (`class/<slug>`, #579). `note` / `effective_year` are
    optional provenance. Existence (the successor must resolve to a live, slugged
    DB entity; the predecessor MAY be dead for the register/variable grain, but the
    classification grain requires it live too — #579) is checked downstream against
    the built DB — this loader stays DB-free.

    #843 representation grain: `predecessor_column` / `successor_column` (TOML
    `from_column` / `to_column`) turn a VARIABLE-grain edge into a REPRESENTATION
    edge — succession between two `(variable_fqid, delivery_column)` pairs (a
    column-level era rename the variable grain can't express, both endpoints
    collapsing to one variable FQID). Both columns are set together or both None;
    a register/variable/classification edge leaves both None. When columns are
    present BOTH endpoints are variable-grain AND share `(provider, register)` —
    a representation rename is INTRA-register (the loader enforces both); the
    variable slug MAY differ (two sibling variables of one register) and MAY be
    equal (same variable, two columns); only the full `(fqid, column)` tuple must
    differ (no self-loop). Both endpoints' `(variable, delivery_column)` must be
    live/observed downstream — the successor-provider skip is total (both
    endpoints share one provider, so no dead-predecessor case arises): a
    within-build column rename observes both columns.

    #846 `variant`: an OPTIONAL `register_variant` slug scoping a REPRESENTATION
    edge's succession to one variant. `""` (the default) = UNSCOPED — the
    whole-variable semantics #843 shipped. A non-empty slug = variant-local: the
    rename holds only within that variant (e.g. FRIDA's firm key, delivered as
    `borgnr` then `persorgnr` then `borgnr` ONLY in `punktskatter-for-energi`,
    while sibling variants deliver `borgnr` continuously). The loader admits
    `variant` ONLY on a representation edge (column fields present) and ONLY with
    `effective_year` (the materializer's time-monotone cycle check orders the
    round-trip by year). The slug is resolved against the built DB at materialize
    time (like the column endpoints), not at load."""

    predecessor: Fqid
    successor: Fqid
    note: str | None
    effective_year: int | None
    predecessor_column: str | None = None
    successor_column: str | None = None
    variant: str = ""


@dataclass(frozen=True)
class CuratedRelatedTo:
    """One `type = "related_to"` "see also" edge: an UNORDERED pair of variable
    FQIDs (`a` / `b`, 3-segment), the curated `relation_kind`, and an optional
    `note`. Endpoint resolution happens at materialize time against the built
    DB."""

    a_provider: str
    a_register: str
    a_variable: str
    b_provider: str
    b_register: str
    b_variable: str
    relation_kind: str
    note: str | None


@dataclass(frozen=True)
class CuratedRelations:
    """The parsed `relations.toml`, grouped by relation kind. One load yields all
    three; the build materializes each into its own table(s)."""

    same_as: tuple[CuratedSameAs, ...]
    replaced_by: tuple[CuratedReplacedBy, ...]
    related_to: tuple[CuratedRelatedTo, ...]


def _join_fqid(provider: str, register: str, variable: str | None) -> str:
    return f"{provider}/{register}" + (f"/{variable}" if variable is not None else "")


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def repo_relations_path() -> Path | None:
    """`reg_meta_build/curation/relations.toml` from a repo checkout, or None
    (wheel installs don't ship curation — it's a maintainer artifact like the
    slug TOMLs). Lives under `curation/` (cross-provider), beside
    `period_family_merges.toml`."""
    candidate = (
        Path(__file__).resolve().parent.parent.parent / "curation" / "relations.toml"
    )
    return candidate if candidate.is_file() else None


_require_fqid_variable = functools.partial(
    require_fqid,
    code="relations_invalid",
    prefix="relations",
    entry_table="[[edge]]",
    file_name="curation/relations.toml",
)


def _reject_foreign_fields(
    entry: dict, edge_type: str, allowed: frozenset[str]
) -> None:
    """Reject any field on `entry` (besides `type`) not legal for `edge_type`. A
    field legal for ANOTHER type (e.g. `effective_year` on a same_as edge) is the
    tell of a mis-typed edge — right fields, wrong `type` — so fail loud."""
    foreign = set(entry) - {"type"} - allowed
    if foreign:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type={edge_type!r} has field(s) "
            f"{sorted(foreign)} not allowed for that type.",
            f"A {edge_type!r} edge accepts {sorted(allowed)} (plus `type`). "
            "Remove the foreign field or fix `type` in "
            "reg_meta_build/curation/relations.toml.",
        )


def _require_note(entry: dict, edge_type: str) -> str | None:
    """`note` is optional but, when present, a non-empty string."""
    note = entry.get("note")
    if note is not None and (not isinstance(note, str) or not note):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type={edge_type!r} `note` must be a non-empty "
            f"string when present, got {note!r}.",
            'Drop `note` or give it a non-empty value like `note = "curated"`.',
        )
    return note


def _classification_fqid(field: str, raw: Any) -> _ClassKey:
    """Parse a 2-segment `provider/classification_slug` classification FQID, used
    by classification-grain same_as. (The 3-seg variable form goes through
    `require_fqid`.)"""
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] `{field}` must be a non-empty FQID string, "
            f"got {raw!r}.",
            'Give a classification FQID like "scb/sun2020".',
        )
    parts = raw.split("/")
    if len(parts) != 2 or not all(parts):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] `{field}` {raw!r} must be a 2-segment "
            "`provider/classification_slug` FQID.",
            'Give a classification FQID like "scb/sun2020".',
        )
    return (parts[0], parts[1])


def _load_same_as(entry: dict) -> CuratedSameAs:
    """Validate one `type = "same_as"` edge. `a` / `b` are FQIDs of the SAME grain
    — both 3-seg variable OR both 2-seg classification. No self-edge."""
    _reject_foreign_fields(entry, "same_as", _SAME_AS_FIELDS)

    def _require_endpoint(field: str) -> str:
        raw = entry.get(field)
        if not isinstance(raw, str) or not raw:
            raise curation_error(
                "relations_invalid",
                f"relations [[edge]] type='same_as' needs `{field}` as a "
                f"non-empty FQID string, got {raw!r}.",
                "Give variable (provider/register/variable) or classification "
                "(provider/classification_slug) FQIDs.",
            )
        return raw

    a_raw = _require_endpoint("a")
    b_raw = _require_endpoint("b")
    a_segs = a_raw.split("/")
    b_segs = b_raw.split("/")
    if len(a_segs) != len(b_segs) or len(a_segs) not in (2, 3):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='same_as' endpoints {a_raw!r} / {b_raw!r} "
            "must be the SAME grain — both 3-segment variable or both 2-segment "
            "classification FQIDs.",
            "same_as relates two entities of one grain; fix the mismatched FQID.",
        )
    note = _require_note(entry, "same_as")
    if len(a_segs) == 3:
        a = _require_fqid_variable(entry, "a")
        b = _require_fqid_variable(entry, "b")
        grain = FqidKind.VARIABLE_BINDING
        edge = CuratedSameAs(
            grain=grain,
            a_provider=a[0],
            a_register=a[1],
            a_variable=a[2],
            b_provider=b[0],
            b_register=b[1],
            b_variable=b[2],
            note=note,
        )
    else:
        a_cls = _classification_fqid("a", a_raw)
        b_cls = _classification_fqid("b", b_raw)
        grain = FqidKind.CLASSIFICATION
        edge = CuratedSameAs(
            grain=grain,
            a_provider=a_cls[0],
            a_register=a_cls[1],
            a_variable=None,
            b_provider=b_cls[0],
            b_register=b_cls[1],
            b_variable=None,
            note=note,
        )
    if (edge.a_provider, edge.a_register, edge.a_variable) == (
        edge.b_provider,
        edge.b_register,
        edge.b_variable,
    ):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='same_as' relates {edge.a_fqid()} to itself.",
            "A same_as edge connects two DISTINCT FQIDs; remove the self-edge.",
        )
    return edge


def _load_replaced_by(entry: dict) -> CuratedReplacedBy:
    """Validate one `type = "replaced_by"` edge. `from` / `to` are FQIDs of the
    SAME grain (register, variable, or classification — not variant). No
    self-loop. Neither endpoint is resolved at load (DB-free); the predecessor may
    be dead for register/variable, but `materialize_curated_replaced_by` requires
    the classification grain's predecessor live too (#579).

    #579: classification endpoints use the `class/<slug>` form (e.g.
    `class/sun1996`), DISAMBIGUATED from the 2-segment register grain
    (`provider/register`). This differs from same_as, whose classification grain
    uses the 2-segment `provider/slug` form — replaced_by can't reuse that without
    colliding with register grain."""
    _reject_foreign_fields(entry, "replaced_by", _REPLACED_BY_FIELDS)
    predecessor = _parse_replaced_by_fqid("from", entry.get("from"))
    successor = _parse_replaced_by_fqid("to", entry.get("to"))
    if predecessor.kind is not successor.kind:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `from` {str(predecessor)!r} "
            f"({predecessor.kind.value}) and `to` {str(successor)!r} "
            f"({successor.kind.value}) are different grains.",
            "Both endpoints must be the same grain (register->register, "
            "variable->variable, or classification->classification).",
        )
    # #843 representation grain: parse optional `from_column` / `to_column`. Both
    # or neither (a representation edge names both endpoints' columns), and only on
    # a VARIABLE-grain edge (succession is column-within-variable). A self-loop is
    # re-keyed on the full `(fqid, column)` tuple below, so same-variable /
    # different-column (the common representation rename) is LEGAL.
    pred_column = _require_column(entry, "from_column")
    succ_column = _require_column(entry, "to_column")
    if (pred_column is None) != (succ_column is None):
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' has exactly one of "
            "`from_column` / `to_column` — a representation edge names BOTH "
            "endpoints' columns.",
            "Give both `from_column` and `to_column` for a representation "
            "(column-grain) edge, or neither for an entity-grain edge.",
        )
    if pred_column is not None and predecessor.kind is not FqidKind.VARIABLE_BINDING:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' carries `from_column` / "
            f"`to_column` but the endpoints are {predecessor.kind.value}-grain.",
            "Representation succession is column-within-variable; column fields "
            "require both endpoints to be variable (provider/register/variable) "
            "FQIDs.",
        )
    # A representation succession is an INTRA-register column rename: the
    # `column_merge` cases it expresses (#846/#196) are all keyed
    # `(register_id, var_id)`, so both endpoints must share `provider` AND
    # `register` (the variable slug MAY differ — two sibling variables of one
    # register — and MAY be equal — same variable, two columns). A cross-register
    # column rename is not a real concept; that's what the variable grain is for.
    # This also makes the materializer's all-live rule safe in partial builds:
    # both endpoints share one provider, so its `if succ.provider not in
    # providers` skip covers the WHOLE edge (no predecessor-provider asymmetry).
    if pred_column is not None and (predecessor.provider, predecessor.register) != (
        successor.provider,
        successor.register,
    ):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' representation edge "
            f"{str(predecessor)!r} -> {str(successor)!r} crosses registers "
            f"({predecessor.provider}/{predecessor.register} -> "
            f"{successor.provider}/{successor.register}).",
            "A representation (column-rename) edge is intra-register: both "
            "endpoints must share provider and register (the variable may "
            "differ). For cross-register succession use the entity (variable) "
            "grain — drop `from_column` / `to_column`.",
        )
    # Case-fold the column in the self-loop check — `from_column = "Col"` /
    # `to_column = "col"` on one variable is a case-only self-loop (the build
    # matches columns case-insensitively), so it must be rejected too.
    pred_col_fold = pred_column.lower() if pred_column is not None else None
    succ_col_fold = succ_column.lower() if succ_column is not None else None
    if (str(predecessor), pred_col_fold) == (str(successor), succ_col_fold):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' self-loop on "
            f"{str(predecessor)!r}"
            + (f" column {pred_column!r}" if pred_column is not None else "")
            + ".",
            "An entity cannot replace itself; remove the edge. (A representation "
            "edge MAY repeat the variable FQID — but then `from_column` and "
            "`to_column` must differ.)",
        )
    note = _require_note(entry, "replaced_by")
    # `note` is provenance-only on a classification edge: that table has no
    # `beskrivning`, so the build stamps the fixed `curated:slug_toml` marker and a
    # human transition reason has nowhere to go. Reject `note` here rather than
    # parse-then-silently-drop it (which reads as a bug) — the reason belongs in a
    # `#` comment above the edges.
    if note is not None and predecessor.kind is FqidKind.CLASSIFICATION:
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' on a classification "
            f"(`class/<slug>`) edge does not accept `note` (got {note!r}).",
            "Drop `note` — a classification edge's `note` is provenance-only "
            "(stamped `curated:slug_toml`). Put the transition reason in a `#` "
            "comment above the edges in reg_meta_build/curation/relations.toml.",
        )
    effective_year = entry.get("effective_year")
    # `isinstance(True, int)` is True in Python — reject a bare bool so an
    # `effective_year = true` typo can't masquerade as the year 1.
    if effective_year is not None and (
        isinstance(effective_year, bool) or not isinstance(effective_year, int)
    ):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `effective_year` must be an "
            f"integer when present, got {type(effective_year).__name__}.",
            "Use a bare integer year, e.g. effective_year = 2012.",
        )
    # #846: optional `variant` scope. Legal ONLY on a representation edge (column
    # fields present) — a `variant` on a plain register/variable/classification
    # edge is a mis-modeled succession (a variant is a delivery coordinate, not an
    # entity-grain curation surface). When set it REQUIRES `effective_year`: a
    # variant-scoped succession may be cyclic (the FRIDA round-trip), and the
    # time-monotone cycle check that permits the cycle orders its edges by year.
    variant = entry.get("variant")
    if variant is not None and (not isinstance(variant, str) or not variant):
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `variant` must be a non-empty "
            f"register_variant slug string when present, got {variant!r}.",
            'Give a register_variant slug like `variant = "punktskatter-for-energi"`,'
            " or drop the field for a variable-level (whole-variable) succession.",
        )
    if variant is not None and pred_column is None:
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' carries `variant` but is not a "
            "representation (column-grain) edge.",
            "`variant` scopes a column-level rename to one register_variant; it is "
            "legal only with `from_column` / `to_column`. Drop `variant`, or add "
            "the column endpoints.",
        )
    if variant is not None and effective_year is None:
        raise curation_error(
            "relations_invalid",
            "relations [[edge]] type='replaced_by' carries `variant` but no "
            "`effective_year`.",
            "A variant-scoped succession may be a time-monotone cycle (a column "
            "left and later returned within the variant); the cycle check orders "
            "it by year. Add `effective_year`, or drop `variant`.",
        )
    return CuratedReplacedBy(
        predecessor=predecessor,
        successor=successor,
        note=note,
        effective_year=effective_year,
        predecessor_column=pred_column,
        successor_column=succ_column,
        variant=variant or "",
    )


def _require_column(entry: dict, field: str) -> str | None:
    """#843: an optional `from_column` / `to_column` field — when present, a
    non-empty string (a delivery-column header). Returns None when absent."""
    raw = entry.get(field)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` must be a "
            f"non-empty string when present, got {raw!r}.",
            'Give a delivery-column name like `from_column = "DispInk04"`, or '
            "drop the field.",
        )
    return raw


def _parse_replaced_by_fqid(field: str, raw: Any) -> Fqid:
    """Parse one replaced_by endpoint FQID string against the FQID grammar,
    restricted to the register / variable / classification grains. The
    classification grain is the `class/<slug>` form (#579)."""
    if not isinstance(raw, str) or not raw:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` must be a "
            f"non-empty FQID string, got {raw!r}.",
            "Quote a register, variable, or classification FQID, e.g. "
            '"scb/lisa", "scb/lisa/kon", or "class/sun1996".',
        )
    try:
        fqid = parse_fqid(raw)
    except FqidError as exc:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` {raw!r} is not a "
            f"valid FQID: {exc}.",
            "Use a register (provider/register), variable "
            "(provider/register/variable), or classification (class/<slug>) FQID.",
        ) from exc
    if fqid.kind not in _REPLACED_BY_GRAINS:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='replaced_by' `{field}` {raw!r} is a "
            f"{fqid.kind.value}-grain FQID; only register, variable, and "
            "classification grains are supported.",
            "Use a 2-segment register, 3-segment variable, or class/<slug> "
            "classification FQID (the variant grain is out of scope).",
        )
    return fqid


def _load_related_to(entry: dict) -> CuratedRelatedTo:
    """Validate one `type = "related_to"` edge. `a` / `b` are 3-seg variable
    FQIDs; `relation_kind` is in `CURATED_RELATION_KINDS` (a non-curated kind is
    rejected — the bulk split siblings are the concept-group fold's, never a
    curated see-also). No self-edge."""
    _reject_foreign_fields(entry, "related_to", _RELATED_TO_FIELDS)
    a = _require_fqid_variable(entry, "a")
    b = _require_fqid_variable(entry, "b")
    if a == b:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='related_to' relates {'/'.join(a)} to itself.",
            "A see-also edge connects two DISTINCT variables; remove the self-edge.",
        )
    kind = entry.get("relation_kind")
    if not isinstance(kind, str) or not kind:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='related_to' needs `relation_kind` as a "
            f"non-empty string, got {kind!r}.",
            f'Use `relation_kind = "<kind>"` with a kind in '
            f"{sorted(CURATED_RELATION_KINDS)}.",
        )
    if kind not in CURATED_RELATION_KINDS:
        raise curation_error(
            "relations_invalid",
            f"relations [[edge]] type='related_to' relation_kind {kind!r} is not "
            f"a curated kind {sorted(CURATED_RELATION_KINDS)} (the bulk auto:triage "
            "split kinds are owned by the concept-group fold, never curated here).",
            "Use a curated relation_kind, or add the new kind to "
            "CURATED_RELATION_KINDS in reg_meta_build/relations.py.",
        )
    note = _require_note(entry, "related_to")
    return CuratedRelatedTo(
        a_provider=a[0],
        a_register=a[1],
        a_variable=a[2],
        b_provider=b[0],
        b_register=b[1],
        b_variable=b[2],
        relation_kind=kind,
        note=note,
    )


def load_relations(path: Path | None) -> CuratedRelations:
    """Parse the single `[[edge]]` array from `relations.toml`, dispatching on
    each entry's `type` to per-type validation. Empty when no file (synthetic
    test builds, wheel installs) or no entries.

    Load-time validation (all EXIT_CONFIG, actionable): `type` is one of
    `same_as` / `replaced_by` / `related_to`; per-type required fields are
    present and well-shaped; a field legal for ANOTHER type is rejected as
    foreign (a mis-typed edge); no self-edge/self-loop; unordered duplicate pairs
    within same_as and related_to are rejected. Endpoint RESOLUTION against the
    built DB is deferred to materialize time (the same load/resolve split as the
    other curation surfaces)."""
    entries = load_curation_entries(
        path,
        entry_key="edge",
        label="relations",
        prefix="relations",
        code_base="relations",
        file_name="curation/relations.toml",
        entry_fields="type + the per-type fields (a/b or from/to)",
    )
    same_as: list[CuratedSameAs] = []
    replaced_by: list[CuratedReplacedBy] = []
    related_to: list[CuratedRelatedTo] = []
    # Unordered FQID pairs already seen per pair-typed kind — a duplicate is
    # curation drift, not something to silently dedup.
    seen_same_as: set[frozenset[str]] = set()
    seen_related: set[frozenset[tuple[str, str, str]]] = set()
    for entry in entries:
        edge_type = entry.get("type")
        if not isinstance(edge_type, str) or edge_type not in _EDGE_TYPES:
            raise curation_error(
                "relations_invalid",
                f"relations [[edge]] has missing/unknown `type` {edge_type!r}.",
                f"Set `type` to one of {sorted(_EDGE_TYPES)} in "
                "reg_meta_build/curation/relations.toml.",
            )
        if edge_type == "same_as":
            edge = _load_same_as(entry)
            pair = frozenset({edge.a_fqid(), edge.b_fqid()})
            if pair in seen_same_as:
                raise curation_error(
                    "relations_invalid",
                    f"relations has a duplicate same_as pair "
                    f"{{{edge.a_fqid()}, {edge.b_fqid()}}}.",
                    "List each pair once (same_as is symmetric — a->b and b->a "
                    "are the same edge).",
                )
            seen_same_as.add(pair)
            same_as.append(edge)
        elif edge_type == "replaced_by":
            replaced_by.append(_load_replaced_by(entry))
        else:  # related_to
            rel = _load_related_to(entry)
            rpair = frozenset(
                {
                    (rel.a_provider, rel.a_register, rel.a_variable),
                    (rel.b_provider, rel.b_register, rel.b_variable),
                }
            )
            if rpair in seen_related:
                raise curation_error(
                    "relations_invalid",
                    f"relations has a duplicate related_to pair "
                    f"{{{'/'.join((rel.a_provider, rel.a_register, rel.a_variable))}, "
                    f"{'/'.join((rel.b_provider, rel.b_register, rel.b_variable))}}}.",
                    "List each variable pair once (the edge is symmetric).",
                )
            seen_related.add(rpair)
            related_to.append(rel)
    return CuratedRelations(
        same_as=tuple(same_as),
        replaced_by=tuple(replaced_by),
        related_to=tuple(related_to),
    )


# ---------------------------------------------------------------------------
# Materialization — same_as
# ---------------------------------------------------------------------------


def _classification_slugs(conn: sqlite3.Connection) -> set[_ClassKey]:
    """Live `(provider, classification_slug)` pairs — the universe a curated
    classification same_as endpoint must resolve into. Classifications carry no
    provider in classifications.toml; the publisher field is the provider."""
    out: set[_ClassKey] = set()
    for slug, publisher in conn.execute(
        "SELECT slug, publisher FROM classification WHERE slug IS NOT NULL"
    ):
        out.add(((publisher or "scb").lower(), slug))
    return out


def _reject_same_as_cycles(edges: list[tuple[Any, Any]], *, label: str) -> None:
    """Reject cycles in the as-declared same_as graph. `edges` holds ONE direction
    per curated pair (the both-directions duplication happens only at DB insert,
    NOT here) — so a node revisited during DFS means the curated edges genuinely
    close a loop, not the harmless reciprocal of an A->B / B->A mirror pair. A node
    is the FQID key tuple. Pure + DB-free; mirrors `reject_replaced_by_cycles`."""
    if not edges:
        return
    adj: dict[Any, list[Any]] = {}
    for a, b in edges:
        adj.setdefault(a, []).append(b)
        adj.setdefault(b, [])
    color: dict[Any, int] = dict.fromkeys(adj, 0)
    parent: dict[Any, Any] = {}

    def visit(node: Any) -> None:
        color[node] = 1
        for nxt in adj[node]:
            if color[nxt] == 1:
                raise curation_error(
                    "relations_same_as_cycle",
                    f"relations {label} forms a cycle through {node!r}.",
                    "same_as must be acyclic as declared; remove the edge that "
                    "closes the loop.",
                )
            if color[nxt] == 0:
                parent[nxt] = node
                visit(nxt)
        color[node] = 2

    for start in list(adj):
        if color[start] == 0:
            visit(start)


def _reject_oversized_components(edges: list[tuple[Any, Any]], *, label: str) -> None:
    """Refuse any same_as edge that would merge two identity components into one
    larger than `_SAME_AS_MAX_COMPONENT` distinct FQIDs (#522). A union-find over
    the undirected edge set; a component above the cap is almost certainly a
    curation error welding distinct concepts, not a real identity cluster. `edges`
    holds ONE direction per curated pair (the both-directions duplication happens
    only at DB insert, NOT here); union-find is direction-agnostic, so a single
    A--B edge suffices to merge the two endpoints' components."""
    parent: dict[Any, Any] = {}

    def find(x: Any) -> Any:
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    sizes: dict[Any, int] = {}
    for node in parent:
        root = find(node)
        sizes[root] = sizes.get(root, 0) + 1
    for size in sizes.values():
        if size > _SAME_AS_MAX_COMPONENT:
            raise curation_error(
                "relations_same_as_component_too_large",
                f"relations {label} forms an identity component of {size} FQIDs "
                f"(cap {_SAME_AS_MAX_COMPONENT}); an edge likely welds two "
                "distinct concepts.",
                "Split the curated same_as cluster — a real identity group is "
                "small. Remove the bridging edge or raise _SAME_AS_MAX_COMPONENT "
                "in reg_meta_build/relations.py if the cluster is genuine.",
            )


def materialize_same_as(
    conn: sqlite3.Connection,
    same_as: Iterable[CuratedSameAs],
    *,
    providers: frozenset[str] = frozenset(),
) -> dict[str, int]:
    """Write curated `same_as` identity edges (both directions) into
    `variable_same_as` / `classification_same_as`. Provider-gated (an edge whose
    endpoint provider isn't built is SKIPPED — a partial `--providers` build
    can't represent it, deferral not drift). Variable endpoints validate
    provider+register existence (the variable slug is NOT checked — same_as is
    slug-anchored); classification endpoints validate `(provider, slug)`
    presence. The combined as-declared graph is cycle-checked and the
    component-size guard refuses a runaway identity cluster, BOTH before any
    INSERT. Returns `{"variable": n, "classification": n}` (one per pair; both
    directions written)."""
    var_edges: list[tuple[_VarKey, _VarKey]] = []
    class_edges: list[tuple[_ClassKey, _ClassKey]] = []
    class_universe: set[_ClassKey] | None = None

    for e in same_as:
        if e.a_provider not in providers or e.b_provider not in providers:
            continue
        if e.grain is FqidKind.VARIABLE_BINDING:
            if resolve_register_id(conn, e.a_provider, e.a_register) is None:
                raise _unknown_same_as_endpoint(e.a_fqid(), "a", "register")
            if resolve_register_id(conn, e.b_provider, e.b_register) is None:
                raise _unknown_same_as_endpoint(e.b_fqid(), "b", "register")
            a_key: _VarKey = (e.a_provider, e.a_register, e.a_variable or "")
            b_key: _VarKey = (e.b_provider, e.b_register, e.b_variable or "")
            var_edges.append((a_key, b_key))
        else:  # CLASSIFICATION
            if class_universe is None:
                class_universe = _classification_slugs(conn)
            a_ck: _ClassKey = (e.a_provider, e.a_register)
            b_ck: _ClassKey = (e.b_provider, e.b_register)
            if a_ck not in class_universe:
                raise _unknown_same_as_endpoint(e.a_fqid(), "a", "classification")
            if b_ck not in class_universe:
                raise _unknown_same_as_endpoint(e.b_fqid(), "b", "classification")
            class_edges.append((a_ck, b_ck))

    _reject_same_as_cycles(var_edges, label="variable same_as")
    _reject_same_as_cycles(class_edges, label="classification same_as")
    _reject_oversized_components(var_edges, label="variable same_as")
    _reject_oversized_components(class_edges, label="classification same_as")

    for a, b in var_edges:
        for src_t, tgt_t in ((a, b), (b, a)):
            conn.execute(
                "INSERT INTO variable_same_as ("
                "a_provider, a_register, a_variable, "
                "b_provider, b_register, b_variable) VALUES (?, ?, ?, ?, ?, ?)",
                (*src_t, *tgt_t),
            )
    for a_k, b_k in class_edges:
        for src_c, tgt_c in ((a_k, b_k), (b_k, a_k)):
            conn.execute(
                "INSERT INTO classification_same_as ("
                "a_provider, a_classification_slug, "
                "b_provider, b_classification_slug) VALUES (?, ?, ?, ?)",
                (*src_c, *tgt_c),
            )
    return {"variable": len(var_edges), "classification": len(class_edges)}


def _unknown_same_as_endpoint(fqid: str, side: str, grain: str) -> Exception:
    return curation_error(
        "relations_same_as_unknown_endpoint",
        f"relations same_as edge endpoint {fqid!r} names a {grain} that does "
        "not exist in this build.",
        f"Fix the `{side}` FQID in reg_meta_build/curation/relations.toml.",
    )


# ---------------------------------------------------------------------------
# Materialization — related_to
# ---------------------------------------------------------------------------


def materialize_related_to(
    conn: sqlite3.Connection,
    related_to: Iterable[CuratedRelatedTo],
    *,
    providers: frozenset[str] = frozenset(),
) -> int:
    """Write curated "see also" edges (both directions) into `variable_related_to`
    on the curated (non-foldable) relation-kind vocabulary. Provider-gated (an
    out-of-build endpoint is SKIPPED). An edge whose providers ARE built but whose
    variable doesn't resolve IS drift -> fail fast. A PK collision with an
    existing edge (auto:triage sibling or another curated edge) is curation drift,
    not a benign re-add -> fail loud. Returns the row count inserted (both
    directions counted)."""
    n_inserted = 0
    for e in related_to:
        if e.a_provider not in providers or e.b_provider not in providers:
            continue
        a_fqid = f"{e.a_provider}/{e.a_register}/{e.a_variable}"
        b_fqid = f"{e.b_provider}/{e.b_register}/{e.b_variable}"
        if resolve_variable_id(conn, e.a_provider, e.a_register, e.a_variable) is None:
            raise curation_error(
                "relations_related_to_unresolved",
                f"relations related_to edge endpoint {a_fqid!r} does not resolve "
                "to a variable.",
                "Fix the `a` FQID in reg_meta_build/curation/relations.toml.",
            )
        if resolve_variable_id(conn, e.b_provider, e.b_register, e.b_variable) is None:
            raise curation_error(
                "relations_related_to_unresolved",
                f"relations related_to edge endpoint {b_fqid!r} does not resolve "
                "to a variable.",
                "Fix the `b` FQID in reg_meta_build/curation/relations.toml.",
            )
        note = e.note if e.note is not None else _CURATED_RELATED_NOTE_DEFAULT
        # Plain INSERT (NOT OR IGNORE): a PK collision is curation drift, not a
        # benign re-add — fail loud rather than silently drop the curated
        # kind/note.
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
                "relations_related_to_collision",
                f"relations related_to curated edge {{{a_fqid}, {b_fqid}}} "
                "collides with an edge already present (auto:triage sibling or "
                "another curated edge).",
                "Remove the duplicate edge from "
                "reg_meta_build/curation/relations.toml.",
            ) from exc
        n_inserted += 2
    return n_inserted


# ---------------------------------------------------------------------------
# Materialization — replaced_by (combined with the event-derived pass)
# ---------------------------------------------------------------------------


def reject_replaced_by_cycles(edges: list[tuple[Any, Any]]) -> None:
    """Reject directed cycles in a `replaced_by` succession graph.

    `edges` is a list of `(predecessor_node, successor_node)` pairs; a node is any
    hashable key (the build passes the FQID slug tuple — register node
    `(provider, register)`, variable node `(provider, register, variable)`). A
    cyclic succession graph has no terminal successor, so the webapp's
    successors()/predecessors() walks would contradict each other.

    Pure + DB-free so it's testable in isolation. The build runs it on the
    COMBINED per-grain graph (event-derived edges + curated edges to insert) — a
    curated edge can close a cycle with an event-derived one, which a curated-only
    view can't see."""
    if not edges:
        return
    adj: dict[Any, list[Any]] = {}
    for a, b in edges:
        adj.setdefault(a, []).append(b)
        adj.setdefault(b, [])

    # WHITE = 0 unvisited, GRAY = 1 on current DFS stack, BLACK = 2 done.
    color: dict[Any, int] = dict.fromkeys(adj, 0)
    parent: dict[Any, Any] = {}

    def visit(node: Any) -> None:
        color[node] = 1
        for nxt in adj[node]:
            if color[nxt] == 1:
                # Reconstruct the cycle for a useful error.
                cycle = [nxt, node]
                cur = node
                while parent.get(cur) is not None and parent[cur] != nxt:
                    cur = parent[cur]
                    cycle.append(cur)
                cycle.append(nxt)
                raise curation_error(
                    "replaced_by_cycle",
                    "relations replaced_by forms a succession cycle: "
                    f"{' -> '.join(repr(n) for n in reversed(cycle))}.",
                    "A succession chain must be acyclic (it needs a terminal "
                    "successor); remove the edge that closes the loop.",
                )
            if color[nxt] == 0:
                parent[nxt] = node
                visit(nxt)
        color[node] = 2

    for start in list(adj):
        if color[start] == 0:
            visit(start)


def _strongly_connected_components(
    adj: dict[Any, list[tuple[Any, int | None]]],
) -> list[set[Any]]:
    """Tarjan's strongly-connected-components over a year-annotated adjacency map
    (the year on each edge is ignored here — only reachability matters). Returns one
    `set` per SCC. Pure + DB-free.

    Iterative (explicit stack) so a deep succession chain can't blow Python's
    recursion limit. Every node a cycle passes through lands in an SCC of size ≥2;
    a self-loop lands in a size-1 SCC (the caller then inspects its in-component
    edges). A node on no cycle is its own singleton SCC."""
    index_of: dict[Any, int] = {}
    low: dict[Any, int] = {}
    on_stack: set[Any] = set()
    stack: list[Any] = []
    components: list[set[Any]] = []
    counter = 0

    for root in adj:
        if root in index_of:
            continue
        # work stack of (node, iterator over its successors)
        work: list[tuple[Any, Any]] = [(root, iter(adj[root]))]
        index_of[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, it = work[-1]
            descended = False
            for nxt, _year in it:
                if nxt not in index_of:
                    index_of[nxt] = low[nxt] = counter
                    counter += 1
                    stack.append(nxt)
                    on_stack.add(nxt)
                    work.append((nxt, iter(adj[nxt])))
                    descended = True
                    break
                if nxt in on_stack:
                    low[node] = min(low[node], index_of[nxt])
            if descended:
                continue
            # node fully explored: if it's a root of an SCC, pop the component.
            if low[node] == index_of[node]:
                comp: set[Any] = set()
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.add(w)
                    if w == node:
                        break
                components.append(comp)
            work.pop()
            if work:
                parent_node = work[-1][0]
                low[parent_node] = min(low[parent_node], low[node])
    return components


def reject_nonmonotone_representation_cycles(
    edges: list[tuple[Any, Any, int | None]],
) -> None:
    """Reject NON-time-monotone cycles in a representation `replaced_by` graph
    (#846), permitting a faithful temporal round-trip.

    `edges` is a list of `(predecessor_node, successor_node, effective_year)`; a
    node is the full representation key `(provider, register, variable, column,
    variant)`. UNLIKE the topological `reject_replaced_by_cycles` (used for the
    entity grains, which must be strictly acyclic), a representation succession MAY
    be cyclic when scoped to one variant and the cycle is a time-monotone
    round-trip: a column left and LATER returned (FRIDA's firm key
    `borgnr -(2014)-> persorgnr -(2018)-> borgnr` within `punktskatter-for-energi`).

    A cycle is PERMITTED iff it is a SINGLE simple time-monotone round-trip: the
    nodes/edges in the cyclic region form exactly one elementary cycle whose edges'
    `effective_year`s are all present and all DISTINCT and admit a single consistent
    forward ordering — rotating the cycle so it starts at its earliest-year edge
    yields strictly increasing years (one wrap at the close). For the 2-cycle (the
    only real case today) this reduces to "two distinct present years". REJECTED,
    all EXIT_CONFIG with actionable messages:
      - any edge in the cyclic region lacks `effective_year` (ordering undefined),
      - two edges in the cyclic region share an `effective_year` (ambiguous /
        impossible round-trip),
      - the distinct years don't form a single monotone wrap (impossible multi-wrap
        order), or
      - the cyclic region is more tangled than one elementary cycle (multiple
        interleaved cycles, or a node with two intra-region successors — can't be a
        clean round-trip).

    Completeness: the cyclic region is found via strongly-connected components
    (Tarjan). EVERY non-trivial SCC (≥2 nodes, or a self-loop) is validated — there
    is no DFS short-circuit that lets a later, more complex cycle slip past once an
    earlier monotone cycle has finished a shared node (the white/gray/black DFS this
    replaced had exactly that gap: an edge into a finished node fell through
    unchecked). A self-loop (`A -> A`) is a 1-node SCC with an in-component edge, so
    it is caught and rejected.

    A NON-cyclic graph (e.g. RTB's single variable-level edge) has only trivial SCCs
    and passes unchanged. Pure + DB-free so it's testable in isolation, like
    `reject_replaced_by_cycles`."""
    if not edges:
        return
    # adjacency keeps the year on each forward edge so a detected cycle can be
    # validated against the round-trip rule (not merely rejected).
    adj: dict[Any, list[tuple[Any, int | None]]] = {}
    for a, b, year in edges:
        adj.setdefault(a, []).append((b, year))
        adj.setdefault(b, [])

    def _reject(nodes: list[Any], years: list[int | None], reason: str) -> None:
        path = " -> ".join(repr(n) for n in nodes)
        raise curation_error(
            "replaced_by_cycle",
            f"relations replaced_by forms a NON-monotone representation "
            f"succession cycle ({reason}): {path} (years {years}).",
            "A variant-scoped representation cycle is permitted only as a SINGLE "
            "time-monotone round-trip — every edge needs a DISTINCT `effective_year` "
            "ordering the return, and the cyclic columns must form exactly one loop. "
            "Give each edge a distinct year, untangle multiple loops, or remove the "
            "edge that closes the loop.",
        )

    def _validate_component(nodes: set[Any]) -> None:
        # `nodes` is one strongly-connected component (≥2 nodes, or a 1-node SCC
        # with a self-loop) — it contains at least one cycle. The ONLY permitted
        # shape is a single elementary cycle: every node has exactly one successor
        # INSIDE the component (in-degree/out-degree 1), so the in-component edges
        # form one simple loop visiting all `nodes` once. Anything else (a node with
        # two in-component successors → interleaved cycles, or a self-loop) is
        # rejected before the year check.
        in_edges: list[tuple[Any, Any, int | None]] = [
            (a, b, year) for a in nodes for b, year in adj[a] if b in nodes
        ]
        ordered = sorted(nodes, key=repr)
        # A self-loop is an in-component edge a -> a; reject it explicitly (it can't
        # be a temporal round-trip — a column can't succeed itself).
        if any(a == b for a, b, _ in in_edges):
            _reject(ordered, [], "a column succeeds itself (self-loop)")
        succ_in: dict[Any, list[tuple[Any, int | None]]] = {n: [] for n in nodes}
        for a, b, year in in_edges:
            succ_in[a].append((b, year))
        if any(len(s) != 1 for s in succ_in.values()):
            _reject(
                ordered,
                [],
                "the cyclic columns don't form a single loop (a column has "
                "multiple in-cycle successors — interleaved round-trips)",
            )
        # Out-degree 1 for every node in a strongly-connected set ⇒ exactly one
        # elementary cycle covering all nodes. Walk it from an arbitrary start to
        # recover the ordered nodes + their edge-years.
        start = ordered[0]
        cycle_nodes = [start]
        cycle_years: list[int | None] = []
        cur = start
        while True:
            nxt, year = succ_in[cur][0]
            cycle_years.append(year)
            if nxt == start:
                cycle_nodes.append(nxt)
                break
            cycle_nodes.append(nxt)
            cur = nxt
        # `cycle_nodes` = [start, ..., start] (closed); `cycle_years[i]` is the year
        # of the edge cycle_nodes[i] -> cycle_nodes[i+1].
        if any(y is None for y in cycle_years):
            _reject(cycle_nodes, cycle_years, "an edge lacks effective_year")
        if len(set(cycle_years)) != len(cycle_years):
            _reject(cycle_nodes, cycle_years, "two edges share an effective_year")
        # Rotate to start at the minimum-year edge; a faithful round-trip is then
        # strictly increasing (one wrap, already cut at the min). `cycle_years` has
        # no None/dups past the guards above.
        years = [y for y in cycle_years if y is not None]
        rot = years.index(min(years))
        rotated = years[rot:] + years[:rot]
        if any(rotated[i] >= rotated[i + 1] for i in range(len(rotated) - 1)):
            _reject(
                cycle_nodes, cycle_years, "years are not a single monotone round-trip"
            )

    for component in _strongly_connected_components(adj):
        # Trivial SCC (1 node, no self-loop) has no cycle; a self-loop's SCC is a
        # single node WITH an in-component edge, caught inside _validate_component.
        if len(component) > 1 or any(
            b in component for b, _ in adj[next(iter(component))]
        ):
            _validate_component(component)


def _slugged_register_fqids(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    return {
        (p_slug, r_slug)
        for r_slug, p_slug in conn.execute(
            "SELECT r.slug, p.slug FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE r.slug IS NOT NULL"
        )
    }


def _slugged_variable_fqids(conn: sqlite3.Connection) -> set[tuple[str, str, str]]:
    return {
        (p_slug, r_slug, v_slug)
        for v_slug, r_slug, p_slug in conn.execute(
            "SELECT v.slug, r.slug, p.slug FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL"
        )
    }


def _slugged_register_variant_keys(
    conn: sqlite3.Connection,
) -> set[tuple[str, str, str]]:
    """#846: live `(provider_slug, register_slug, variant_slug)` triples — the
    universe a curated `variant` scope must resolve into. A register_variant slug
    is register-scoped, so a variant resolves iff its slug names a slugged variant
    OF the edge's register. Variant slugs are NOT case-folded (unlike delivery
    columns): they are curated canonical slugs, not drifting SCB headers."""
    return {
        (p_slug, r_slug, rv_slug)
        for rv_slug, r_slug, p_slug in conn.execute(
            "SELECT rv.slug, r.slug, p.slug FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE rv.slug IS NOT NULL AND r.slug IS NOT NULL"
        )
    }


def _slugged_representation_keys(
    conn: sqlite3.Connection,
) -> set[tuple[str, str, str, str]]:
    """#843: live `(provider_slug, register_slug, variable_slug, delivery_column)`
    quadruples — the universe a curated representation endpoint must resolve into.
    A representation = a `(variable, delivery_column)` PAIR; the observed delivery
    columns of a variable live in `variable_alias.delivery_column_name`, so an
    endpoint resolves iff its variable is live AND it names an OBSERVED column of
    that variable. Unlike the register/variable grain (dead predecessor allowed),
    BOTH endpoints must be live (a within-build column rename observes both
    columns).

    The column is LOWERCASED in the returned set so the membership check is
    case-INSENSITIVE — SCB delivery headers drift in case across deliveries. The
    fold is done in PYTHON (`str.lower`, Unicode-aware) so it agrees with the
    materializer's `edge.*_column.lower()` lookup keys: SQLite `LOWER()` is
    ASCII-only (`LOWER('Ägare')` == `'Ägare'`), so folding the column SQL-side here
    would mismatch the materializer on Swedish åäö headers and falsely flag the
    edge. The caller lowercases the curator's column before the membership check
    while STORING the verbatim TOML value."""
    return {
        (p_slug, r_slug, v_slug, col.lower())
        for v_slug, r_slug, p_slug, col in conn.execute(
            "SELECT v.slug, r.slug, p.slug, a.delivery_column_name "
            "FROM variable_alias a "
            "JOIN variable v ON a.variable_id = v.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL"
        )
    }


def _slugged_representation_variant_keys(
    conn: sqlite3.Connection,
) -> set[tuple[str, str, str, str, str]]:
    """#846: live `(provider_slug, register_slug, variable_slug, delivery_column,
    variant_slug)` quintuples — the variant-AWARE liveness universe a variant-scoped
    representation endpoint must resolve into.

    `variable_alias` is keyed by `register_variant_id`, so the SAME variable can
    observe different columns in different delivering variants. The variable-level
    `_slugged_representation_keys` collapses that — it only proves a column is
    observed SOMEWHERE in the register. A variant-scoped edge must additionally
    prove the endpoint is observed IN THE NAMED variant, else a mistyped `variant`
    whose columns happen to live in a SIBLING variant would write a false
    variant-local edge. This set carries the variant slug so the scoped check can
    require the column live in THAT register_variant.

    The column is Unicode-lowercased Python-side (same rationale as
    `_slugged_representation_keys` — agree with the materializer's `.lower()` keys,
    NOT SQLite's ASCII-only `LOWER()`); the variant slug is kept verbatim (a curated
    canonical slug, not a drifting header)."""
    return {
        (p_slug, r_slug, v_slug, col.lower(), rv_slug)
        for v_slug, r_slug, p_slug, col, rv_slug in conn.execute(
            "SELECT v.slug, r.slug, p.slug, a.delivery_column_name, rv.slug "
            "FROM variable_alias a "
            "JOIN variable v ON a.variable_id = v.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "JOIN register_variant rv "
            "ON a.register_variant_id = rv.register_variant_id "
            "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL AND rv.slug IS NOT NULL"
        )
    }


def _slugged_classification_slugs(conn: sqlite3.Connection) -> set[str]:
    """Live `classification.slug` values — the universe a curated classification
    successor must resolve into (#579). A classification slug is GLOBALLY unique
    (no provider segment), mirroring the `classification_replaced_by` PK."""
    return {
        slug
        for (slug,) in conn.execute(
            "SELECT slug FROM classification WHERE slug IS NOT NULL"
        )
    }


def _existing_classification_succession(
    conn: sqlite3.Connection,
) -> set[tuple[str, str]]:
    """`(predecessor_slug, successor_slug)` of rows already in
    `classification_replaced_by` — the #571 auto edges, inserted before this pass.
    A curated edge on the same pair dedups against these (and the pending curated
    ones)."""
    return {
        (pred, succ)
        for pred, succ in conn.execute(
            "SELECT predecessor_slug, successor_slug FROM classification_replaced_by"
        )
    }


def _unresolved_curated_successor(successor: Fqid, grain_noun: str) -> Exception:
    return curation_error(
        "replaced_by_unresolved_successor",
        f"Curated replaced_by successor {str(successor)!r} does not resolve to a "
        f"live, slugged {grain_noun} in this build.",
        f"A curated successor must exist; fix the FQID or add the {grain_noun} slug.",
    )


def _unresolved_curated_predecessor(predecessor: Fqid, grain_noun: str) -> Exception:
    return curation_error(
        "replaced_by_unresolved_predecessor",
        f"Curated replaced_by predecessor {str(predecessor)!r} does not resolve to "
        f"a live, slugged {grain_noun} in this build.",
        f"Classification succession is all-live (the read side depends on it); "
        f"both endpoints must exist. Fix the FQID or add the {grain_noun}.",
    )


def _unresolved_curated_representation(
    fqid: Fqid, column: str, side: str, variant: str = ""
) -> Exception:
    """#843: a representation endpoint `(variable_fqid, delivery_column)` whose
    variable isn't live OR whose column isn't an observed delivery column of that
    live variable in this build. Both endpoints must be live (within-build column
    rename), so this fires for either side.

    #846: for a variant-scoped edge, liveness is checked IN the named `variant` (the
    column must be observed in THAT register_variant, not merely register-wide); the
    message names the variant so a column live only in a sibling variant is
    diagnosable."""
    scope = f" in variant {variant!r}" if variant else ""
    return curation_error(
        "replaced_by_unresolved_representation",
        f"Curated replaced_by {side} representation "
        f"{str(fqid)!r} column {column!r} does not resolve to a live variable "
        f"with that observed delivery column{scope} in this build.",
        "A representation succession edge is all-live (both columns are observed "
        "within the build" + (", in the named variant" if variant else "") + "). "
        "Fix the FQID / column"
        + (" / variant" if variant else "")
        + ", or drop the edge in reg_meta_build/curation/relations.toml.",
    )


def _unresolved_curated_variant(fqid: Fqid, variant: str) -> Exception:
    """#846: a `variant` scope whose slug doesn't name a live register_variant of
    the edge's register."""
    return curation_error(
        "replaced_by_unresolved_variant",
        f"Curated replaced_by representation edge on {str(fqid)!r} scopes to "
        f"variant {variant!r}, which is not a live register_variant of that "
        "register in this build.",
        "A variant-scoped representation succession must name a real "
        "register_variant slug of the edge's register. Fix the slug, or drop "
        "`variant` (for a variable-level rename) in "
        "reg_meta_build/curation/relations.toml.",
    )


def materialize_curated_replaced_by(
    conn: sqlite3.Connection,
    edges: Iterable[CuratedReplacedBy],
    seen_register: set[tuple[str, str, str, str]],
    seen_variable: set[tuple[str, str, str, str, str, str]],
    *,
    providers: frozenset[str],
    progress: Any,
) -> dict[str, int]:
    """Materialize curated `replaced_by` succession edges (#440 — now from
    `relations.toml`). Runs right after the event-derived pass
    (`_materialize_replaced_by_edges` in db.py), SHARING its `seen_register` /
    `seen_variable` PK sets so a curated edge dedups against an event-derived one
    (and against another curated row). The rows are parsed/shape-validated DB-free
    by `load_relations`; this pass does the DB-aware existence checks, the
    COMBINED-graph cycle check, and the INSERTs.

    Acyclicity: the load-time check sees only the curated edges, so it can't catch
    a curated edge that closes a cycle WITH an event-derived edge (event A->B +
    curated B->A). This pass reconstructs the event edges from the shared `seen_*`
    PK tuples and runs `reject_replaced_by_cycles` on the combined per-grain graph
    (event + curated-to-insert) BEFORE any INSERT, so a cycle aborts cleanly.

    Resolution rules: the SUCCESSOR must resolve to a live, slugged DB entity (a
    non-resolving successor is a CURATION ERROR -> fail fast), EXCEPT a successor
    whose PROVIDER isn't in this (partial) build, which is SKIPPED. For the
    register/variable grains the PREDECESSOR MAY be dead — inserted VERBATIM
    (slug-anchored); its provider is never gated. The CLASSIFICATION grain is the
    exception: its predecessor must ALSO be live (see the #579 arm below). For
    register/variable grains `note = 'curated:slug_toml'` marks provenance and the
    row's own `note` lands in `beskrivning`.

    #579 classification arm: classifications are GLOBAL — no provider segment on
    the `class/<slug>` form, so classification edges are NOT provider-gated (the
    inactive-provider skip never fires for them). BOTH endpoints must resolve to a
    live `classification` row (fail-fast otherwise) — unlike register/variable,
    where a dead predecessor is allowed. Classification succession is ALL-LIVE by
    design: the read side (`classification_chain`) and the validator's
    `_check_classification_replaced_by` `dangling` check both require both endpoints
    live, so a dead-predecessor edge would otherwise fail LATE at validation (CLI)
    or ship a dangling row (`--no-validate`). Edges land in
    `classification_replaced_by (predecessor_slug,
    successor_slug, effective_year, note)` — that table has NO `beskrivning`, so
    there is nowhere for a transition reason: `note` is provenance-only, stamped
    with the same `curated:slug_toml` marker as the register/variable arms (the
    auto #571 rows carry `derived:vintage_chain`), and the human reason lives in a
    `#` comment in relations.toml (the loader rejects `note` on these edges). The
    auto #571 edges already exist (`derive_classification_succession` runs earlier
    in the build), so dedup is by `(predecessor_slug, successor_slug)` against the
    table + the pending curated edges, and the cycle check runs over the COMBINED
    slug graph. The 1→many split (one predecessor, several successors) is
    intentional and supported.

    #843 representation arm: an edge carrying `predecessor_column` /
    `successor_column` (set by `_load_replaced_by` only on a variable-grain edge)
    is a REPRESENTATION succession — a column-level era rename between two
    `(variable_fqid, delivery_column)` pairs (the fact the variable grain can't
    express). Curated-only (no auto representation grain), so the cycle graph is
    built from the curated representation edges alone, keyed on the full
    `(provider, register, variable, column)` tuple, and dedup is against the
    pending set alone (no auto source pre-populates the table — `seen_representation`
    starts empty). BOTH endpoints must resolve to a live variable WITH the named
    observed delivery column; an unresolved endpoint fails fast. Because the loader
    pins both endpoints to the SAME `(provider, register)`, the successor-provider
    skip is total — it covers the whole edge, so no dead-predecessor case arises
    (unlike the register/variable grain, where a cross-provider predecessor may be
    dead). The inactive-provider skip still applies (successor provider not in this
    partial build, which now also means the predecessor's). Edges land
    in `representation_replaced_by` with `note = 'curated:slug_toml'` and the row's
    own `note` in `beskrivning` (same convention as the register/variable arms).

    #846 variant scope: an edge may carry a `variant` register_variant slug (`''` =
    variable-level, the #843 default) scoping the succession to ONE variant. It is
    threaded into the cycle node key (so a variant-scoped node is DISTINCT from the
    variable-level one on the same column) and stored in the new `variant` column.
    The slug must resolve to a live register_variant of the edge's register
    (fail-fast, like the column endpoints). A variant-scoped edge's ENDPOINTS are
    further checked variant-aware: each column must be observed IN the named variant
    (`_slugged_representation_variant_keys`), not merely register-wide — else a
    mistyped variant whose columns live in a sibling variant would write a false
    edge. Unscoped (`''`) endpoints keep the register-wide check.

    The cycle check is PARTITIONED by scope: variant-scoped edges go through the
    year-aware `reject_nonmonotone_representation_cycles`, which PERMITS a single
    time-monotone round-trip (a column left and later returned within the variant,
    e.g. FRIDA's `borgnr -> persorgnr -> borgnr`) while rejecting a same-year /
    missing-year / tangled cycle; unscoped (variable-level) edges go through the
    strictly-acyclic `reject_replaced_by_cycles` (a variable-level round-trip is a
    hard curation error, never a permitted return).

    Returns `{"register", "variable", "classification", "representation",
    "skipped_duplicate", "skipped_inactive_provider"}`."""
    edges = list(edges)
    if not edges:
        return {
            "register": 0,
            "variable": 0,
            "classification": 0,
            "representation": 0,
            "skipped_duplicate": 0,
            "skipped_inactive_provider": 0,
        }

    progress("Materializing curated replaced_by edges from relations.toml...")
    live_registers = _slugged_register_fqids(conn)
    live_variables = _slugged_variable_fqids(conn)
    # Classification universe + combined succession graph are built lazily — only
    # when a classification edge is actually present (most builds carry none).
    live_classifications: set[str] | None = None
    classification_cycle_edges: list[tuple[str, str]] | None = None
    seen_classification: set[tuple[str, str]] | None = None
    # #843 representation universe — built lazily (most builds carry no
    # representation edge). Node key is the full `(provider, register, variable,
    # column, variant)` tuple (#846 adds the trailing variant segment). UNLIKE the
    # classification arm, there is NO table read to
    # seed `seen_representation`: `representation_replaced_by` has no auto/event
    # source — this pass is its sole writer, run once per build — so it is always
    # empty at entry. The within-batch dedup below (add `rpk` to the pending set)
    # is sufficient.
    live_representations: set[tuple[str, str, str, str]] | None = None
    # #846 variant universe — `(provider, register, variant_slug)` of live
    # register_variants — built lazily, only when a variant-scoped edge appears.
    live_variants: set[tuple[str, str, str]] | None = None
    # #846 variant-AWARE endpoint liveness — `(provider, register, variable, column,
    # variant_slug)` of observed columns PER delivering variant. A variant-scoped
    # edge's endpoints must be live in the NAMED variant, not just register-wide
    # (`live_representations`); built lazily alongside `live_variants`.
    live_representation_variants: set[tuple[str, str, str, str, str]] | None = None
    # #846: the node key gains a trailing `variant` segment (`''` = variable-level),
    # so a variant-scoped node is DISTINCT from the variable-level one on the same
    # column. The cycle edges carry `effective_year` for the time-monotone check.
    representation_cycle_edges: list[
        tuple[
            tuple[str, str, str, str, str],
            tuple[str, str, str, str, str],
            int | None,
        ]
    ] = []
    # 10-tuple: the two 5-element `(provider, register, variable, column, variant)`
    # node keys concatenated (`*pred_key, *succ_key`).
    seen_representation: set[
        tuple[str, str, str, str, str, str, str, str, str, str]
    ] = set()

    # Reconstruct the event-derived edges from the shared seen-set PK tuples
    # snapshotted at pass entry, so the combined-graph cycle check sees them:
    #   register PK (pp, pr, sp, sr)         -> (pp, pr) -> (sp, sr)
    #   variable PK (pp, pr, pv, sp, sr, sv) -> (pp, pr, pv) -> (sp, sr, sv)
    register_cycle_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = [
        (pk[:2], pk[2:]) for pk in seen_register
    ]
    variable_cycle_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = [
        (pk[:3], pk[3:]) for pk in seen_variable
    ]

    n_skipped_duplicate = 0
    n_skipped_inactive_provider = 0
    pending_register: list[tuple] = []
    pending_variable: list[tuple] = []
    pending_classification: list[tuple] = []
    pending_representation: list[tuple] = []

    for edge in edges:
        pred = edge.predecessor
        succ = edge.successor
        # #843 representation grain: a variable-grain edge carrying both columns is
        # a column-level era rename. Curated-only — no auto source, no provider
        # gate beyond the standard successor-provider check below — and ALL-LIVE
        # (both endpoints' observed delivery columns must exist). Handle it before
        # the entity arms (it's a variable-grain FQID, but the column fields make
        # it a distinct grain). Enter on EITHER column set, not only the
        # predecessor's: a malformed internal edge with one column would otherwise
        # fall through to the variable arm and silently drop it; the both-set
        # assertion below then fails LOUDLY on a one-sided edge instead of
        # mis-routing (the loader's both-or-neither rule already bars this from
        # TOML — this guards internally-constructed edges).
        if edge.predecessor_column is not None or edge.successor_column is not None:
            assert (
                edge.predecessor_column is not None
                and edge.successor_column is not None
            )  # both-or-neither
            assert (
                succ.provider is not None
                and succ.register is not None
                and succ.variable is not None
                and pred.provider is not None
                and pred.register is not None
                and pred.variable is not None
            )
            if succ.provider not in providers:
                n_skipped_inactive_provider += 1
                continue
            if live_representations is None:
                live_representations = _slugged_representation_keys(conn)
            # #846: an optional `variant` scope. `''` = variable-level (the #843
            # default); a slug must resolve to a live register_variant of THIS
            # edge's register (both endpoints share one register, enforced at load),
            # mirroring the all-live endpoint rule. The variant rides the node key
            # (case-SENSITIVE — curated canonical slug, not a drifting column), so a
            # variant-scoped node is DISTINCT from the variable-level one and a
            # variant-scoped cycle is permitted by the time-monotone check while the
            # variable-level grain stays strictly acyclic.
            if edge.variant:
                if live_variants is None:
                    live_variants = _slugged_register_variant_keys(conn)
                if (pred.provider, pred.register, edge.variant) not in live_variants:
                    raise _unresolved_curated_variant(pred, edge.variant)
                if live_representation_variants is None:
                    live_representation_variants = _slugged_representation_variant_keys(
                        conn
                    )
            # Match the column case-INSENSITIVELY (SCB headers drift in case; the
            # retired `column_merge` surface this replaced also case-folded its TOML
            # columns), but
            # STORE the curator's VERBATIM column value (the build folds with Python
            # `str.lower` downstream — the live set above and validate.py's
            # `py_lower` UDF — so storing verbatim is safe). Folding is Python-side,
            # Unicode-aware, so Swedish åäö headers match (SQLite `LOWER()` is
            # ASCII-only). The lowercased keys drive membership, dedup (`rpk`), and
            # the cycle-graph nodes, so case-variant duplicates collapse and
            # case-only cycles are caught; the pending row keeps the verbatim
            # columns. The `variant` segment is appended verbatim.
            pred_key = (
                pred.provider,
                pred.register,
                pred.variable,
                edge.predecessor_column.lower(),
                edge.variant,
            )
            succ_key = (
                succ.provider,
                succ.register,
                succ.variable,
                edge.successor_column.lower(),
                edge.variant,
            )
            # Endpoint liveness. For an UNSCOPED edge (`variant=''`, a variable-level
            # rename that needn't be in any one variant) the register-wide
            # `live_representations` 4-tuple is correct. For a VARIANT-scoped edge the
            # endpoints must be observed IN THAT variant — checking only the 4-tuple
            # would pass an endpoint live only in a SIBLING variant (a mistyped
            # `variant`), so check the variant-aware 5-tuple instead.
            if edge.variant:
                assert live_representation_variants is not None  # built above
                if (*pred_key[:4], edge.variant) not in live_representation_variants:
                    raise _unresolved_curated_representation(
                        pred, edge.predecessor_column, "predecessor", edge.variant
                    )
                if (*succ_key[:4], edge.variant) not in live_representation_variants:
                    raise _unresolved_curated_representation(
                        succ, edge.successor_column, "successor", edge.variant
                    )
            else:
                if pred_key[:4] not in live_representations:
                    raise _unresolved_curated_representation(
                        pred, edge.predecessor_column, "predecessor"
                    )
                if succ_key[:4] not in live_representations:
                    raise _unresolved_curated_representation(
                        succ, edge.successor_column, "successor"
                    )
            rpk = (*pred_key, *succ_key)
            if rpk in seen_representation:
                n_skipped_duplicate += 1
                continue
            seen_representation.add(rpk)
            representation_cycle_edges.append((pred_key, succ_key, edge.effective_year))
            pending_representation.append(
                (
                    pred.provider,
                    pred.register,
                    pred.variable,
                    edge.predecessor_column,  # verbatim — match-lower, store-verbatim
                    succ.provider,
                    succ.register,
                    succ.variable,
                    edge.successor_column,  # verbatim
                    edge.variant,
                    edge.effective_year,
                    _REPLACED_BY_NOTE_CURATED,
                    edge.note,
                )
            )
            continue
        # Classification grain (#579) is GLOBAL — `class/<slug>` has no provider,
        # so it's NOT provider-gated (and `succ.provider` is None, which would
        # trip the provider assertions below). Handle it before the provider gate.
        if succ.kind is FqidKind.CLASSIFICATION:
            assert succ.classification is not None and pred.classification is not None
            if live_classifications is None:
                live_classifications = _slugged_classification_slugs(conn)
                seen_classification = _existing_classification_succession(conn)
                classification_cycle_edges = list(seen_classification)
            assert seen_classification is not None
            assert classification_cycle_edges is not None
            if succ.classification not in live_classifications:
                raise _unresolved_curated_successor(succ, "classification")
            # Classification succession is all-live (the read side
            # `classification_chain` and the validator's dangling check both
            # require both endpoints live), so — UNLIKE the register/variable
            # grain — the predecessor may NOT be dead. Fail fast here rather than
            # fail late at validation (CLI) or ship a dangling row (--no-validate).
            if pred.classification not in live_classifications:
                raise _unresolved_curated_predecessor(pred, "classification")
            cpk = (pred.classification, succ.classification)
            if cpk in seen_classification:
                n_skipped_duplicate += 1
                continue
            seen_classification.add(cpk)
            classification_cycle_edges.append(cpk)
            # `note` is provenance-only (the table has no `beskrivning`): stamp the
            # fixed `curated:slug_toml` marker, mirroring the register/variable arms
            # so curated rows are distinguishable from the auto `derived:vintage_chain`
            # ones. The human transition reason lives in a `#` comment in the TOML
            # (the loader rejects `note` on a classification edge).
            pending_classification.append(
                (
                    pred.classification,
                    succ.classification,
                    edge.effective_year,
                    _REPLACED_BY_NOTE_CURATED,
                )
            )
            continue
        assert succ.provider is not None
        if succ.provider not in providers:
            n_skipped_inactive_provider += 1
            continue
        if succ.kind is FqidKind.REGISTER:
            assert succ.provider is not None and succ.register is not None
            assert pred.provider is not None and pred.register is not None
            succ_key = (succ.provider, succ.register)
            if succ_key not in live_registers:
                raise _unresolved_curated_successor(succ, "register")
            pk = (pred.provider, pred.register, succ.provider, succ.register)
            if pk in seen_register:
                n_skipped_duplicate += 1
                continue
            seen_register.add(pk)
            register_cycle_edges.append(((pred.provider, pred.register), succ_key))
            pending_register.append(
                (*pk, edge.effective_year, _REPLACED_BY_NOTE_CURATED, edge.note)
            )
        else:  # FqidKind.VARIABLE_BINDING (classification handled above)
            assert (
                succ.provider is not None
                and succ.register is not None
                and succ.variable is not None
            )
            assert (
                pred.provider is not None
                and pred.register is not None
                and pred.variable is not None
            )
            succ_key = (succ.provider, succ.register, succ.variable)
            if succ_key not in live_variables:
                raise _unresolved_curated_successor(succ, "variable")
            pk = (
                pred.provider,
                pred.register,
                pred.variable,
                succ.provider,
                succ.register,
                succ.variable,
            )
            if pk in seen_variable:
                n_skipped_duplicate += 1
                continue
            seen_variable.add(pk)
            variable_cycle_edges.append(
                ((pred.provider, pred.register, pred.variable), succ_key)
            )
            pending_variable.append(
                (*pk, edge.effective_year, _REPLACED_BY_NOTE_CURATED, edge.note)
            )

    reject_replaced_by_cycles(register_cycle_edges)
    reject_replaced_by_cycles(variable_cycle_edges)
    # Classification: cycle-check the COMBINED slug graph (auto #571 edges +
    # pending curated) before any INSERT. None when no classification edge was
    # seen — `reject_replaced_by_cycles([])` is a no-op anyway, but the lazy build
    # skips the table read entirely for the common no-classification build.
    if classification_cycle_edges is not None:
        reject_replaced_by_cycles(classification_cycle_edges)
    # #843/#846 representation: curated-only, so the cycle graph is just the
    # pending representation edges (no event/auto source to combine with), keyed on
    # the full `(provider, register, variable, column, variant)` node.
    #
    # PARTITION by scope before checking. A cycle's nodes always share one variant
    # scope (each edge stamps one `variant` on BOTH its endpoints — index 4 of the
    # node key — so a cycle can't mix scopes). The two scopes get DIFFERENT rules:
    #   - UNSCOPED (`variant=''`, the variable-level grain) must be STRICTLY ACYCLIC
    #     like the entity grains — a variable-level `A->B (2014)` + `B->A (2018)` is
    #     a hard curation error, NOT a permitted round-trip — so it goes through the
    #     topological `reject_replaced_by_cycles`. (The year is dropped; acyclicity
    #     is year-independent.)
    #   - VARIANT-scoped may be a faithful time-monotone round-trip (FRIDA's
    #     `borgnr -> persorgnr -> borgnr` within one variant), so it goes through the
    #     year-aware `reject_nonmonotone_representation_cycles`.
    # Both checkers no-op on an empty list.
    unscoped_representation_edges = [
        (pred_key, succ_key)
        for pred_key, succ_key, _year in representation_cycle_edges
        if pred_key[4] == ""
    ]
    variant_representation_edges = [
        (pred_key, succ_key, year)
        for pred_key, succ_key, year in representation_cycle_edges
        if pred_key[4] != ""
    ]
    reject_replaced_by_cycles(unscoped_representation_edges)
    reject_nonmonotone_representation_cycles(variant_representation_edges)

    conn.executemany(
        "INSERT INTO register_replaced_by ("
        "predecessor_provider, predecessor_register, "
        "successor_provider, successor_register, "
        "effective_year, note, beskrivning) VALUES (?, ?, ?, ?, ?, ?, ?)",
        pending_register,
    )
    conn.executemany(
        "INSERT INTO variable_replaced_by ("
        "predecessor_provider, predecessor_register, predecessor_variable, "
        "successor_provider, successor_register, successor_variable, "
        "effective_year, note, beskrivning) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        pending_variable,
    )
    conn.executemany(
        "INSERT INTO classification_replaced_by ("
        "predecessor_slug, successor_slug, effective_year, note) "
        "VALUES (?, ?, ?, ?)",
        pending_classification,
    )
    conn.executemany(
        "INSERT INTO representation_replaced_by ("
        "predecessor_provider, predecessor_register, predecessor_variable, "
        "predecessor_column, "
        "successor_provider, successor_register, successor_variable, "
        "successor_column, "
        "variant, effective_year, note, beskrivning) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        pending_representation,
    )
    n_register = len(pending_register)
    n_variable = len(pending_variable)
    n_classification = len(pending_classification)
    n_representation = len(pending_representation)

    progress(
        f"  {n_register:,} register / {n_variable:,} variable / "
        f"{n_classification:,} classification / "
        f"{n_representation:,} representation curated replaced_by edges "
        f"({n_skipped_duplicate:,} dedup-collapsed, "
        f"{n_skipped_inactive_provider:,} skipped — successor provider "
        f"not in this build)"
    )
    return {
        "register": n_register,
        "variable": n_variable,
        "classification": n_classification,
        "representation": n_representation,
        "skipped_duplicate": n_skipped_duplicate,
        "skipped_inactive_provider": n_skipped_inactive_provider,
    }


# ---------------------------------------------------------------------------
# Derivation — variable vintage succession (#584, lifts classification editions)
# ---------------------------------------------------------------------------


def derive_variable_vintage_succession(
    conn: sqlite3.Connection, *, progress: Any | None = None
) -> int:
    """Lift `classification_replaced_by` EDITION succession (#571) to the variable
    grain through value-set bindings (#584, clean tier).

    Two variables A, B in the SAME register whose value-set classifications C_A,
    C_B are ADJACENT in a `classification_replaced_by` chain — and that are
    otherwise the same series (same `variable.name`) — mint
    `variable_replaced_by` (A → B) with `effective_year` from the classification
    edge and `note = 'derived:classification_vintage_lift'`. Adjacent-chain (the
    edge's predecessor→successor verbatim, NOT predecessor→latest), mirroring
    `concept_groups.derive_classification_succession`.

    Family key = `(register_id, variable.name)`. Only the **clean tier** fires:
    a family where the chained editions map UNAMBIGUOUSLY 1:1 to variables — a
    bijection edition↔variable. Two guards drop everything else:

      - **Entangled** (out of scope, #488 et al.): an edition bound by >1
        variable in the family (näringsgren / parental utbildningsnivå /
        fordonsreg / fek / rams Näringsgren cross-products). A same-name key
        alone would cross-link parallel variants, so the whole family is skipped.
      - **Interval-native** (#271, no lift owed): a single variable spanning >1
        chained edition across its own states. That variable already carries the
        lineage in ONE `variable_id`; the family is skipped (the variable appears
        under >1 edition, breaking the bijection).

    Level separation is free: each level binds its own classification lineage
    (`sni2007-grov` ≠ `sni2007-utokad`), so the lift over distinct slugs never
    crosses grov into utokad — no special level handling.

    Dedup: an edge already in `variable_replaced_by` (curated #375/#440 or auto
    `timeseries_event`) WINS — `INSERT OR IGNORE` against the PK leaves it
    untouched. The pass runs AFTER `_materialize_replaced_by_edges`, so those
    rows already exist and `variable.slug` is populated. Caller guards under
    `skip_slugs` (every slug is NULL there). Returns the count of edges minted."""
    # Classification edition edges: predecessor_slug → (successor_slug, year).
    edition_edges = conn.execute(
        "SELECT predecessor_slug, successor_slug, effective_year "
        "FROM classification_replaced_by"
    ).fetchall()
    if not edition_edges:
        if progress is not None:
            progress("  0 variable vintage-lift edges (no classification chains)")
        return 0

    # Slugs that participate in any edition chain — restrict the family edition
    # map to these so a variable's unrelated classifications don't break the
    # bijection (only chained editions matter for the lift).
    chain_slugs: set[str] = set()
    for pred, succ, _year in edition_edges:
        chain_slugs.add(pred)
        chain_slugs.add(succ)

    # Per (register_id, variable.name) family: edition_slug → {variable_id}, and
    # variable_id → {edition_slug}. Built from the live state→classification
    # bindings, restricted to chained editions.
    family_edition_vars: dict[tuple[int, str], dict[str, set[int]]] = {}
    family_var_editions: dict[tuple[int, str], dict[int, set[str]]] = {}
    rows = conn.execute(
        "SELECT DISTINCT v.register_id, v.name, vs.variable_id, c.slug "
        "FROM variable_state vs "
        "JOIN variable v ON v.variable_id = vs.variable_id "
        "JOIN classification c ON c.id = vs.classification_id "
        "WHERE vs.classification_id IS NOT NULL "
        "  AND c.slug IS NOT NULL "
        "  AND v.name IS NOT NULL "
        "  AND v.slug IS NOT NULL"
    ).fetchall()
    for register_id, name, variable_id, slug in rows:
        if slug not in chain_slugs:
            continue
        key = (register_id, name)
        family_edition_vars.setdefault(key, {}).setdefault(slug, set()).add(variable_id)
        family_var_editions.setdefault(key, {}).setdefault(variable_id, set()).add(slug)

    # Resolve a variable_id to its FQID slug tuple (provider, register, variable)
    # for the edge endpoints. The lift only links live, slugged variables.
    fqid_of: dict[int, tuple[str, str, str]] = {
        r[0]: (r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT v.variable_id, p.slug, r.slug, v.slug "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL AND p.slug IS NOT NULL"
        )
    }

    pending: list[tuple[str, str, str, str, str, str, int | None]] = []
    for key in sorted(family_edition_vars):
        edition_vars = family_edition_vars[key]
        var_editions = family_var_editions[key]
        # Bijection guard: every chained edition the family touches must bind
        # exactly ONE variable (else entangled cross-product), and every variable
        # must bind exactly ONE chained edition (else interval-native / spans
        # editions in one variable_id). A family failing either is skipped whole.
        if any(len(vids) != 1 for vids in edition_vars.values()):
            continue  # entangled: an edition bound by >1 variable
        if any(len(slugs) != 1 for slugs in var_editions.values()):
            continue  # interval-native / multi-edition variable
        # Clean bijection. Walk each classification edge whose BOTH endpoints are
        # bound in this family and mint the adjacent variable edge.
        for pred_slug, succ_slug, year in edition_edges:
            if pred_slug not in edition_vars or succ_slug not in edition_vars:
                continue
            pred_vid = next(iter(edition_vars[pred_slug]))
            succ_vid = next(iter(edition_vars[succ_slug]))
            pred_fqid = fqid_of.get(pred_vid)
            succ_fqid = fqid_of.get(succ_vid)
            if pred_fqid is None or succ_fqid is None:
                continue
            if pred_fqid == succ_fqid:
                continue  # same variable both ends — no self-edge
            pending.append((*pred_fqid, *succ_fqid, year))

    # INSERT OR IGNORE: a curated (#375/#440) or auto (timeseries_event) edge on
    # the same PK already present WINS — the derived row is silently dropped, never
    # clobbering richer provenance. `executemany` skips on PK collision per row.
    conn.executemany(
        "INSERT OR IGNORE INTO variable_replaced_by ("
        "predecessor_provider, predecessor_register, predecessor_variable, "
        "successor_provider, successor_register, successor_variable, "
        "effective_year, note) VALUES (?, ?, ?, ?, ?, ?, ?, "
        f"'{_REPLACED_BY_NOTE_VINTAGE_LIFT}')",
        pending,
    )
    # The lift is the ONLY `variable_replaced_by` writer that inserts AFTER the
    # curated/auto passes have run, so it's the one writer that must re-check the
    # COMBINED graph for cycles: a pre-existing reversed edge `B -> A` (curated,
    # auto, or contradictory source) plus a lift's chain-direction `A -> B` closes
    # a cycle that the earlier passes couldn't see (the lift edge didn't exist
    # yet). Read back the full graph and reuse `reject_replaced_by_cycles` so any
    # cycle the lift closed — with any auto/curated edge, possibly multi-hop —
    # fails the build loudly rather than shipping a graph with no terminal
    # successor (which breaks the webapp's terminal-successor walk). The node key
    # is the variable FQID slug tuple, matching the existing combined check.
    full_graph = [
        ((row[0], row[1], row[2]), (row[3], row[4], row[5]))
        for row in conn.execute(
            "SELECT predecessor_provider, predecessor_register, "
            "predecessor_variable, successor_provider, successor_register, "
            "successor_variable FROM variable_replaced_by"
        )
    ]
    reject_replaced_by_cycles(full_graph)

    # Count the note-stamped rows so the return is authoritative regardless of
    # how many `INSERT OR IGNORE` rows collided with a pre-existing edge.
    n_minted = conn.execute(
        "SELECT COUNT(*) FROM variable_replaced_by WHERE note = ?",
        (_REPLACED_BY_NOTE_VINTAGE_LIFT,),
    ).fetchone()[0]
    if progress is not None:
        n_dropped = len(pending) - n_minted
        progress(
            f"  {n_minted:,} variable vintage-lift edges "
            f"({n_dropped:,} dedup-collapsed onto curated/auto edges)"
        )
    return n_minted
