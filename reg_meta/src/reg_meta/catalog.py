"""Catalog: FQID-to-row resolution against the reg_meta SQLite DB.

Implements the FQID API (see DESIGN.md → Catalog API surface): ``Catalog.resolve(fqid)``
turns any FQID kind into a typed entity row (the 3-segment binding arm resolves
to the longitudinal ``ResolvedVariable``), with ``resolve_at`` + the per-edge
accessors for period/relationship traversal.

A2.6: the binding FQID is 3-segment (`provider/register/slug`). Variant and
period are delivery coordinates passed to ``resolve_at`` (not FQID segments),
and the variant / register_version FQID kinds — plus the ``editions`` discovery
path that enumerated per-edition bindings — are gone (see DESIGN.md → FQID grammar).
"""

from __future__ import annotations

import json
from collections import deque
from typing import TYPE_CHECKING, Literal, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field

from .db import (
    classification_succession_as_of_year,
    classification_succession_edge_is_active,
    db_path_from_args,
    open_db,
)
from .doc_db import (
    RelatedDocument,  # noqa: TC001 - Pydantic resolves model fields at runtime.
)
from .errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from .fqid import (
    DEFAULT_VARIANT_SLUG,
    Fqid,
    FqidError,
    FqidKind,
    parse,
    period_token_for_bounds,
    period_token_to_bounds,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from .graph import RelationshipGraph

# Succession-chain tuple grain (variable triple, register pair, or classification
# 1-tuple); shared so `_walk_terminal` preserves arity across grains (see
# `resolve_terminal_successor`).
_SuccTuple = TypeVar("_SuccTuple", tuple[str, str, str], tuple[str, str], tuple[str])

_CLASSIFICATION_FAMILY_LABELS = {
    "icd": "ICD",
    "lkf": "LKF",
    "sni": "SNI",
    "ssyk": "SSYK",
}
_VariantFamilyInfo = tuple[str, str]  # (family_key, display_label)


def _classification_family_key(slug: str) -> str | None:
    """Stable browse-family key for one-dimensional succession chains."""
    for key in _CLASSIFICATION_FAMILY_LABELS:
        if slug == key or slug.startswith(f"{key}-"):
            return key
        suffix = slug.removeprefix(key)
        if suffix != slug and suffix[:1].isdigit():
            return key
    return None


def _variant_family_label(labels: Iterable[str]) -> str:
    """Display label for a register-variant succession family.

    LISA's `Individer, 15 år och äldre` / `Individer, 16 år och äldre` shape is
    the first consumer: the common pre-comma stem is the family label. Fallback to
    the full common label when labels already match, then to the first label for
    non-uniform families.
    """
    ordered = [label for label in labels if label]
    if not ordered:
        return ""
    if all(label == ordered[0] for label in ordered):
        return ordered[0]
    stems = [label.split(",", 1)[0].strip() for label in ordered]
    if stems[0] and all(stem == stems[0] for stem in stems):
        return stems[0]
    return ordered[0]


class _CatalogModel(BaseModel):
    """Frozen Pydantic base for the catalog return surface (#681): the
    `Resolved*` / `*Summary` / `*Ref` / edition / coverage / group / tag models
    the webapp consumes as response models (collapsing its 1:1 wrappers in a
    follow-up). Frozen preserves the immutability the prior `@dataclass(frozen=True)`
    gave; `Fqid` fields ride the `Fqid.__get_pydantic_core_schema__` hook (wire =
    the canonical FQID string). Constructed by KEYWORD (Pydantic takes no
    positional args).

    `populate_by_name` + `extra="forbid"` are hoisted here so the
    `register`-aliasing register-bearing models don't each repeat them, and so a
    typo'd kwarg fails loudly (restoring the prior `@dataclass`'s fail-fast).
    `serialize_by_alias` makes a direct `model_dump()` / `model_dump_json()` emit
    the public wire key (`register`, not the internal `register_name` attr the
    `BaseModel.register`-method clash forced; #681) — keeping a library/CLI dump
    aligned with the FastAPI response path, which already serializes `by_alias`.
    Harmless on the alias-free models. Mirrors reg_schema's `_Model` shape
    (frozen + extra-forbid + populate_by_name) — a separate base by design:
    reg_meta must NOT depend on reg_schema."""

    model_config = ConfigDict(
        frozen=True,
        populate_by_name=True,
        extra="forbid",
        serialize_by_alias=True,
    )


class ResolvedProvider(_CatalogModel):
    fqid: Fqid
    provider_id: int
    name: str


class ResolvedRegister(_CatalogModel):
    fqid: Fqid
    register_id: int
    provider_id: int
    # Glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): `name` was `registernamn`, `purpose` was `registersyfte`.
    # `registerrubrik` is dropped (redundant with name).
    name: str
    purpose: str | None
    related_documents: tuple[RelatedDocument, ...] = ()
    tags: tuple[TagMembership, ...] = ()


class ResolvedClassification(_CatalogModel):
    fqid: Fqid
    classification_id: int
    short_name: str
    name: str
    # OUTBOUND succession (#571): the editions that replaced this one
    # (`classification_replaced_by`, keyed on this edition's slug). Empty for a
    # terminal (current) edition.
    replaced_by: tuple[ClassificationRef, ...] = ()
    # NON-temporal derivation / variant refs (#779). `derived_from` names the
    # source classifications this specialized classification derives from;
    # `derivatives` names classifications derived from this one. These do not
    # affect edition succession, terminal walks, or supersedes_id.
    derived_from: tuple[ClassificationDerivedFromRef, ...] = ()
    derivatives: tuple[ClassificationDerivedFromRef, ...] = ()
    via_same_as: tuple[Fqid, ...] | None = None


# Catalog-tree children shapes (A5.1b-i; see DESIGN.md → Catalog API surface): thin child nodes for the webapp's
# catalog browse (`/api/catalog` → providers → registers → bindings). Each carries
# the addressable `Fqid` (webapp serializes `str(fqid)`) + a display `name`,
# mirroring the `Resolved*` style but without the per-entity detail those carry.
class ProviderSummary(_CatalogModel):
    fqid: Fqid
    name: str


class RegisterSummary(_CatalogModel):
    fqid: Fqid
    name: str
    purpose: str | None


class BindingSummary(_CatalogModel):
    fqid: Fqid
    name: str | None


class CatalogSizes(_CatalogModel):
    """Headline catalog-size counts — browse-addressable (slugged)
    providers/registers/variables; the grain the catalog listings render."""

    providers: int
    registers: int
    variables: int


# The open-ended `variable_state.valid_to` sentinel (the reg_meta_build DDL
# default). A window ending here is "ongoing" — it has no finite upper bound.
OPEN_ENDED_VALID_TO = "9999-12-31"

# The unknown/open-START `variable_state.valid_from` sentinel (scb.py's final
# fallback). A window starting here has no known finite lower bound — the mirror of
# `OPEN_ENDED_VALID_TO` on the start side.
UNKNOWN_VALID_FROM = "0001-01-01"


class VariableCoverage(_CatalogModel):
    """Coverage aggregate for one variable over its `variable_state` windows
    (#351): the study-window signal a browse row needs without resolving every
    state. `coverage_from` is the earliest `valid_from`; `coverage_to` the latest
    FINITE `valid_to` (None when the latest window is open-ended — see
    `open_ended` — or when the variable has no states); `state_count` > 1 inside a
    window signals a break worth surfacing. A variable with no states has
    `state_count == 0` and both bounds None (distinct from open-ended)."""

    coverage_from: str | None
    coverage_to: str | None
    open_ended: bool
    state_count: int


class RegisterCoverage(_CatalogModel):
    """Coverage aggregate for one register (#351): `variable_count` is its
    slugged (browsable) variables; the span is over ALL their states.
    `coverage_to`/`open_ended` follow `VariableCoverage`."""

    variable_count: int
    coverage_from: str | None
    coverage_to: str | None
    open_ended: bool


def _coverage_bounds(
    cov_from: str | None, cov_to: str | None
) -> tuple[str | None, str | None, bool]:
    """Map a `(MIN(valid_from), MAX(valid_to))` SQL aggregate to
    `(coverage_from, coverage_to, open_ended)`. The open-ended sentinel as the
    max means "ongoing": `coverage_to` is None and `open_ended` True. NULLs (a
    stateless variable / empty register) yield both bounds None and `open_ended`
    False."""
    open_ended = cov_to == OPEN_ENDED_VALID_TO
    return cov_from, (None if open_ended else cov_to), open_ended


# Concept-group browse shapes (#303; see DESIGN.md → Concept groups): a derived
# PRESENTATION-ONLY grouping of near-identical browse rows (split-sibling edge
# components, month-suffixed families, curated facet families). A group is NOT an
# FQID-addressable entity — members carry the real leaf FQIDs; the group is a
# fold-and-pick affordance for browse surfaces. Classification VINTAGE editions
# (lkf1980…lkf2026, ssyk1996→ssyk2012) are NOT folded here — they surface as
# succession edges in `classification_replaced_by` (#571).
class GroupFacet(_CatalogModel):
    """One facet assignment on a group member: `axis` names the dimension
    ('month' / 'rank' / 'enhet' …) when the group declares one, or None for an
    AXIS-LESS group — a curated classification umbrella (SUN/ISCED/NordDRG), whose
    members are distinct classifications carrying their own short label, not points
    on a shared scale. A multi-axis member (#819) carries one `GroupFacet` per
    declared axis, ordered by the axis's ordinal. `value` sorts (zero-padded where
    needed), `label` displays."""

    axis: str | None
    value: str
    label: str


class GroupAxis(_CatalogModel):
    """One declared facet axis of a concept group (#819): the stable `name` (the
    derivation/match key — `concept_group_axis.axis`, the same string a member's
    `GroupFacet.axis` carries) and its curator-authored display `label`
    (`concept_group_axis.label`, e.g. "Hushållsbegrepp"). Consumers MATCH on `name`
    (it keys `GroupFacet.axis`) and DISPLAY `label` — the label is presentation
    only, never an identity. `ConceptGroupSummary.axes` is a tuple of these, ordered
    by the axis's `ordinal`; empty for edge / axis-less umbrella groups."""

    name: str
    label: str


class ConceptGroupMember(_CatalogModel):
    """A group member: the leaf's FQID (binding or classification), its display
    name, and its facet assignments (empty on edge-group members). `delivery_column`
    is None for a whole-variable member and the SCB delivery column for a
    REPRESENTATION member (#819) — a multi-axis family can carry two members on one
    variable (two delivery columns), so two members may share an `fqid` and are then
    distinguished by `delivery_column`. Additive + defaulted so existing callers /
    the webapp TS stay valid."""

    fqid: Fqid
    name: str | None
    facets: tuple[GroupFacet, ...]
    delivery_column: str | None = None


class ConceptGroupSummary(_CatalogModel):
    """One derived concept group. `key` is the scope-unique derivation key
    (slug stem / min member slug / curated key) — a stable anchor for UI
    state, not an FQID. `axes` holds the group's ordered facet axes (#819) as
    `GroupAxis(name, label)` — empty for edge / axis-less umbrella groups, one for
    token/single-axis-curated groups, N for a multi-axis curated family (the iot
    disposable-income group). Each axis carries its stable match `name` and its
    curator-authored display `label` (consumers match on `name`, display `label`).
    Members are ordered by their first facet value, then slug."""

    key: str
    label: str
    # The derivation dimension. The DB writer constrains it to these three
    # (`concept_group.source` CHECK in reg_meta_build/db.py); narrowed to the
    # Literal (#681) so it is the tight API contract the webapp consumes directly
    # (the prior `str` was looser than the webapp wrapper, now collapsed).
    source: Literal["edge", "token", "curated"]
    axes: tuple[GroupAxis, ...]
    members: tuple[ConceptGroupMember, ...]
    tags: tuple[TagMembership, ...] = ()


# The three `concept_group.source` values the DB CHECK permits (reg_meta_build/db.py).
_GROUP_SOURCES: frozenset[str] = frozenset({"edge", "token", "curated"})


def _group_source(raw: str) -> Literal["edge", "token", "curated"]:
    """Validate a `concept_group.source` DB string against the three allowed values
    at the read boundary (#681). The build's CHECK constraint already enforces this,
    so a violation is a corrupt-DB/build-invariant break — fail fast rather than
    emit an off-contract `ConceptGroupSummary.source`. The `cast` is sound: the guard
    above proves membership in the Literal."""
    if raw not in _GROUP_SOURCES:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="corrupt_concept_group_source",
            error_class="query",
            message=f"concept_group.source has unexpected value {raw!r}",
            remediation="Rebuild the reg_meta DB; the source CHECK constraint is violated.",
        )
    return cast('Literal["edge", "token", "curated"]', raw)


class BindingGroupRef(_CatalogModel):
    """The concept group a binding belongs to, as the group's addressable
    `(provider, register, key)` (#616). Carried by `ResolvedVariable.group` so a
    member URL renders group-aware without a second fetch. A variable belongs to at
    most one group (validated, #819) — even a multi-axis family that carries the
    variable under several REPRESENTATION members keeps them in one group — so this
    stays a singular ref (the read uses `SELECT DISTINCT` since a representation
    variable now yields several member rows). The full member list lives behind
    `Catalog.concept_group(provider, register, key)`.
    `key` is `ConceptGroupSummary.key` (the scope-unique derivation key), not an
    FQID segment.

    `register` is a `BaseModel` method, so the Python attr is `register_name`
    aliased to the `register` wire/init name (#681); construct/serialize as
    `register`, read as `.register_name`."""

    provider: str
    register_name: str = Field(alias="register")
    key: str


class TagSummary(_CatalogModel):
    """One curated thematic tag (#311) in the global vocabulary. `slug` is the
    globally-unique tag id; `member_count` / `starred_count` are this tag's total
    members and the subset flagged golden/recommended (across both grains)."""

    slug: str
    label: str
    description: str | None
    member_count: int
    starred_count: int


class TagMembership(_CatalogModel):
    """A tag a register/variable belongs to (#311), from its side: the tag's
    `slug`/`label`, plus THIS membership's `rank` (curated order within the tag),
    `starred` (golden/recommended flag) and one-line `note` rationale."""

    slug: str
    label: str
    rank: int
    starred: bool
    note: str | None


def _tag_membership(row: sqlite3.Row) -> TagMembership:
    """Build a `TagMembership` from a `(slug, label, rank, starred, note)` row.
    `starred` is stored as INTEGER 0/1 — coerce to bool."""
    return TagMembership(
        slug=row["slug"],
        label=row["label"],
        rank=row["rank"],
        starred=bool(row["starred"]),
        note=row["note"],
    )


def _tag_note_key(row: sqlite3.Row) -> tuple[int, int, int, str]:
    note = row["note"]
    starred = bool(row["starred"])
    note_bucket = 0 if starred and note else 1 if note else 2
    return (note_bucket, row["rank"], row["member_variable_id"], row["slug"])


def _aggregate_tag_memberships(
    rows: Iterable[sqlite3.Row],
) -> tuple[TagMembership, ...]:
    """Collapse member-grain tag rows into one thematic tag per slug.

    The lowest rank orders the group tag, any starred member makes the group tag
    recommended, and a note from the strongest contributing membership is retained
    for the group-level surface.
    """
    tags: dict[str, dict[str, object]] = {}
    for row in rows:
        current = tags.get(row["slug"])
        starred = bool(row["starred"])
        if current is None:
            tags[row["slug"]] = {
                "slug": row["slug"],
                "label": row["label"],
                "rank": row["rank"],
                "starred": starred,
                "note": row["note"],
                "_note_key": _tag_note_key(row),
            }
            continue
        current["rank"] = min(cast("int", current["rank"]), row["rank"])
        current["starred"] = bool(current["starred"]) or starred
        note_key = _tag_note_key(row)
        if row["note"] and note_key < cast(
            "tuple[int, int, int, str]", current["_note_key"]
        ):
            current["note"] = row["note"]
            current["_note_key"] = note_key
    return tuple(
        TagMembership(
            slug=cast("str", tag["slug"]),
            label=cast("str", tag["label"]),
            rank=cast("int", tag["rank"]),
            starred=cast("bool", tag["starred"]),
            note=cast("str | None", tag["note"]),
        )
        for tag in sorted(
            tags.values(),
            key=lambda t: (cast("int", t["rank"]), cast("str", t["slug"])),
        )
    )


# A2.5b variant-browser shape (see reg_webapp/DESIGN.md → Catalog router structure): a variant is a register sub-resource, NOT
# an FQID-addressable node (the variant left the binding FQID; see DESIGN.md → Two-level variable model), so this
# carries the variant `slug` (the `?variant=` browse coordinate) + display fields,
# not an `Fqid`. A4.4c adds the panel-shape columns (read-only; see reg_webapp/DESIGN.md → Catalog router structure): a
# `panel_entity_key` that is a bare variable-slug str or a tuple of slugs
# (composite), the `panel_time_key` ("period", a variable-slug, or a tuple of
# slugs (composite)), and the `panel_time_grain` ('delivery'/'row'). Most
# variants carry no panel data → all three are None.
class PopulationMetadata(_CatalogModel):
    """SCB population prose for one register version (#799)."""

    name: str
    definition: str | None
    comment: str | None
    date_range: str | None


class ObjectTypeMetadata(_CatalogModel):
    """SCB object-type prose for one register version (#799)."""

    name: str
    definition: str | None


class RegisterVersionMetadata(_CatalogModel):
    """SCB register-version prose nested under a variant (#799)."""

    name: str | None
    description: str | None
    measurement_information: str | None
    populations: tuple[PopulationMetadata, ...] = ()
    object_types: tuple[ObjectTypeMetadata, ...] = ()


class VariantSummary(_CatalogModel):
    slug: str
    name: str | None
    description: str | None
    display_group: str | None
    variant_family: str | None = None
    variant_family_label: str | None = None
    panel_entity_key: str | tuple[str, ...] | None
    panel_time_key: str | tuple[str, ...] | None
    panel_time_grain: str | None
    versions: tuple[RegisterVersionMetadata, ...] = ()


def _has_text(*values: str | None) -> bool:
    """True when at least one provider-native metadata field carries display text."""
    return any(value not in (None, "") for value in values)


def _decode_panel_entity_key(raw: str | None) -> str | tuple[str, ...] | None:
    """Decode a stored panel key (A4.4c): a JSON-array string → tuple
    (composite key), any other string → itself (simple bare slug / "period"),
    NULL → None. Mirrors the `populate_slugs` writer (json.dumps for the tuple
    case). Generic — reused for both `panel_entity_key` and `panel_time_key`."""
    if raw is None:
        return None
    if raw.startswith("["):
        return tuple(json.loads(raw))
    return raw


# The polymorphic period a caller passes to `resolve_at` (see DESIGN.md → Catalog API surface; see reg_schema/DESIGN.md → Two layers: models vs. validator). Mirrors
# `Source.period`: a bare year (int), a period token ("HT2020"/"2020-Q3"/
# "2020-08"/"2018-12-31"), an explicit range dict {"from", "to"} (endpoints are
# int or token), or the "_default" snapshot sentinel (no period filter). It is a
# delivery coordinate, NOT an FQID segment (the binding FQID is 3-seg; see DESIGN.md → FQID grammar).
Period = int | str | dict


class ValueSetMember(_CatalogModel):
    """One (code, label) entry in a state's value set (#681). Was a bare
    `(code, label)` tuple inside `VariableState.value_set`; promoted to a model so
    the wire serializes named objects (`{"code": ..., "label": ...}`) rather than a
    2-element array. The codes/labels are PUBLIC value-set strings, not row-level
    data."""

    code: str
    label: str


class ClassificationConformance(_CatalogModel):
    """Per-state value-set/classification conformance (#656).

    `declared_classification_*` names the classification asserted by the source
    value-set label. When `status == "severed"`, `VariableState.classification_slug`
    is already None; this object preserves the original declaration plus the
    coverage evidence explaining why the link was cleared."""

    declared_classification_slug: str
    declared_classification_short_name: str
    declared_classification_name: str
    status: Literal["kept", "severed"]
    checked_code_count: int
    matched_code_count: int
    nonconforming_code_count: int
    overlap: float
    nonconforming_codes: tuple[ValueSetMember, ...] = ()


class VariableState(_CatalogModel):
    """One `variable_state` row (see DESIGN.md → Two-level variable model) — a variable's per-delivery shape, tagged
    with the **variant coordinate** it was delivered in. The longitudinal
    `ResolvedVariable.states` is a tuple of these; `resolve_at` returns the
    subset whose validity range intersects the queried period."""

    state_id: int
    # `register_variant.slug` for `register_variant_id`. The two-level model (see DESIGN.md → Two-level variable model) makes the column
    # NOT NULL, so a state always carries a real variant — `variant` is always a
    # resolved slug, never the synth `_default` placeholder (that fiction exists
    # only at variant-slot resolve time, not on a stored state).
    variant: str
    # `register_variant.name` — the variant's curator display name (e.g. "Snöskotrar"
    # for the slug `snoskotrar`), surfaced for DISPLAY so the picker shows the proper
    # name instead of the lowercase/ASCII-folded slug. None for a NULL-named variant
    # (the consumer falls back to the slug). `variant` (the slug) stays the add
    # coordinate; this is display-only and never an identity.
    variant_label: str | None
    # Variant-family metadata (#376). `variant_family` is the stable family key
    # (the terminal/head variant slug), while `variant_family_label` is display
    # text for picker/cart folding. Both are None for variants with no curated
    # succession family; concrete `variant` remains the add coordinate.
    variant_family: str | None = None
    variant_family_label: str | None = None
    register_variant_id: int
    valid_from: str  # ISO 8601 'YYYY-MM-DD', inclusive
    valid_to: str  # ISO 8601 'YYYY-MM-DD', inclusive ('9999-12-31' open-ended)
    data_type: str | None
    data_length: str | None
    # Denormalized latest alias for the state (see DESIGN.md → Two-level variable model); full alias history lives in
    # `variable_alias`.
    delivery_column_name: str | None
    # Raw source attribution/code for this state when SCB source metadata varies
    # by edition. Variable-grain `ResolvedVariable.source_register_text` stays
    # populated only when the source text is stable for the variable.
    source_register_text: str | None
    # State-grain operational definition (#736). Parallel same-period
    # multi-response members may share one variable/value set while each delivery
    # column has its own meaning.
    operational_definition: str | None = None
    # Overlap discriminator (see reg_meta_build/DESIGN.md → Build-time triage (SCB); multi-vintage / grain / coding). NOT NULL
    # DEFAULT '' in the DDL, so '' means "no discriminator", not absent.
    value_set_version_label: str
    value_set_id: int | None
    # `ValueSetMember` (code, label) entries for `value_set_id`, hydrated eagerly
    # when non-NULL. None when the state carries no value set. Eager (frozen model
    # favors it); typical per-state code fan-out is small.
    value_set: tuple[ValueSetMember, ...] | None
    # Variable-grain `variable.is_identifier` denormalized onto every state via a
    # JOIN (constant across a variable's states), so consumers with no
    # ResolvedVariable in scope (the `resolve_at` / `/states` paths) can still
    # read the authoritative identifier flag.
    is_identifier: bool
    # Classification family slug (see DESIGN.md → Classifications) for this state's value set (e.g. 'lkf2007'),
    # resolved per-state from `variable_state.classification_id` — it varies
    # across a variable's states. None for code-less / unclassified states.
    classification_slug: str | None
    classification_conformance: ClassificationConformance | None = None
    # The coarsest exact display token for this window (#321/#681): the token
    # `period_token_for_bounds(valid_from, valid_to)` expands back to exactly
    # `(valid_from, valid_to)`, or the explicit `lo..hi` range for a non-grammar
    # window. None for an open-ended state (the `9999-12-31` sentinel has no finite
    # token — the SPA renders "since valid_from"). Computed in `_row_to_state` so
    # the webapp reads it instead of recomputing (was `_state_model`'s job).
    period_token: str | None = None


class VariableRef(_CatalogModel):
    """A variable-grain edge endpoint (see DESIGN.md → Composite registers and source tracking): the 3-part `(provider, register,
    variable)` identity of a `same_as` / `replaced_by` neighbor. Carried by
    `predecessors`/`successors` and `ResolvedVariable.same_as`/`.replaced_by`.

    A2.6: `fqid` is the neighbor's 3-segment binding FQID — the edge triple IS
    the binding FQID now that the variant/period left the grammar (see DESIGN.md → FQID grammar and Composite registers and source tracking).
    Build-time slug validation guarantees the triple round-trips, so this is
    never None in practice.

    `register` is a `BaseModel` method, so the Python attr is `register_name`
    aliased to the `register` wire/init name (#681); construct/serialize as
    `register`, read as `.register_name` (see `BindingGroupRef`).
    """

    fqid: Fqid | None
    provider: str
    register_name: str = Field(alias="register")
    variable: str
    # #142: succession refs (predecessors/successors) carry the human transition
    # reason (`timeseries_event.beskrivning`) and the AktuellVariabel-grain
    # `effective_year` (the successor edition's year). Both None on `same_as`
    # refs and on bare-Variabel/Register/RegisterVariant-grain succession (no
    # edition → no year; documented asymmetry, reg_meta_build/db.py).
    reason: str | None = None
    effective_year: int | None = None


class ClassificationRef(_CatalogModel):
    """A classification-grain succession edge endpoint (#571): one
    `classification_replaced_by` neighbor of a classification edition. Carried by
    `classification_predecessors`/`classification_successors` and
    `ResolvedClassification.replaced_by`.

    The classification FQID is 2-segment (`class/<slug>`), so the edge endpoint is
    a single slug — no provider/register triple (unlike `VariableRef`). There is
    no `reason`/`beskrivning` column on `classification_replaced_by`: `note`
    carries the build provenance instead (e.g. `derived:vintage_chain`).
    Succession references the EXACT edition slug, so `slug` is the load-bearing
    identity; `fqid` is best-effort (None on a malformed slug, mirroring
    `_ref_fqid`) — succession tolerates dead predecessor editions by design."""

    fqid: Fqid | None
    slug: str
    effective_year: int | None = None
    note: str | None = None


class ClassificationDerivedFromRef(_CatalogModel):
    """A non-temporal classification derivation endpoint (#779).

    Used for links such as KS87-P -> ICD-9-KS87, where the relationship is a
    contemporaneous setting variant rather than edition succession. `note` is
    curator-facing relation text from `classification_derived_from`, not
    provenance for a temporal chain."""

    fqid: Fqid | None
    slug: str
    short_name: str
    name: str
    note: str | None = None


class ClassificationEdition(_CatalogModel):
    """One edition in a classification succession chain (#571), as returned by
    `Catalog.classification_chain`. Unlike `ClassificationRef` (a single edge
    endpoint), this is a fully-hydrated node in the WHOLE chain — the webapp
    browse panel renders the complete edition timeline from a list of these.

    `slug` is the load-bearing identity (succession references the exact edition
    slug). Every chain edition is a LIVE `classification` row —
    `reg_meta_build`'s validator (`validate.py`, the `classification_replaced_by`
    check) fails the build if any succession edge references a slug with no live
    row, so a "dead edition" can't exist in a validated DB. `fqid` is None only on
    a *malformed* slug (a lower-level slug-grammar concern, also build-prevented;
    `_class_ref_fqid` mirrors `ClassificationRef.fqid`'s nullability); `name` comes
    from the live row. `effective_year` is the year on the outbound
    `classification_replaced_by` edge by which this edition is superseded; it can
    be future-dated. `is_current` marks started editions with no active outbound
    successor as of the DB's classification succession policy year; `is_self` marks
    the edition the caller queried (resolved to its canonical live slug when the
    query was a `same_as` alias)."""

    slug: str = Field(description="The edition's literal slug (e.g. 'sun2000').")
    fqid: Fqid | None = Field(
        description="The edition's 2-seg classification FQID, or None only when the "
        "slug is malformed/unresolvable (the build validator guarantees succession "
        "editions are live classification rows; rendered as plain text, not a link)."
    )
    name: str | None = Field(
        description="The edition's display name (every chain edition is a live row)."
    )
    effective_year: int | None = Field(
        description="The year on the succession edge naming this edition as the "
        "predecessor — i.e. the year it is superseded by its successor. Future "
        "successor edges can coexist with `is_current=True` before their policy year."
    )
    version_year: int | None = Field(
        description="The edition's OWN point-in-time vintage year, read off the "
        "`classification` row's `valid_from` (the vintage lives in slug + name + "
        "valid_from). UNLIKE `effective_year` (the supersession year), this is the "
        "edition's intrinsic year — sun1996→1996, sun2020→2020 regardless of "
        "whether it has a successor. None when the row carries no `valid_from`."
    )
    is_current: bool = Field(
        description="True for a started edition with no active outbound successor "
        "as of the DB's classification succession policy year. A 1-to-many split "
        "root's chain can have MULTIPLE such editions (one per active branch tip)."
    )
    is_self: bool = Field(
        description="True for the edition the caller queried (resolved to its "
        "canonical live slug when the query was a same_as alias)."
    )


class ClassificationFamilySummary(_CatalogModel):
    """Stable browse entrypoint for a one-dimensional classification succession family.

    Families are derived from `classification_replaced_by`, not stored as concept
    groups. `key` is the route identity (`/catalog/group/class/<key>`), while
    `editions` is the same fully hydrated chain surface classification leaves use.
    """

    key: str
    label: str
    editions: tuple[ClassificationEdition, ...]


class VariableEdition(_CatalogModel):
    """One edition in a variable succession chain (#582), as returned by
    `Catalog.variable_chain`. The variable-grain dual of `ClassificationEdition`:
    unlike `VariableRef` (a single edge endpoint), this is a fully-hydrated node in
    the WHOLE chain — the webapp browse panel renders the complete variable timeline
    from a list of these.

    The `(provider, register, variable)` triple is the load-bearing identity (the
    binding FQID is exactly that triple now that variant/period left the grammar; see
    DESIGN.md → FQID grammar). A chain edition may be a DEAD/renamed predecessor with
    no live `variable` row — that is the #355/#411 renamed-slug model (catalog.py
    `VariableRef` / `resolve_terminal_successor`): succession tolerates dead
    predecessor editions by design. There is NO `variable_replaced_by` validator
    forbidding it (UNLIKE `ClassificationEdition`, where the classification validator
    — `validate.py`, the `classification_replaced_by` check — DOES fail the build on a
    dead endpoint). A dead edition still carries its (syntactically-valid) binding
    `fqid`, so a citation 301-redirects to the current edition (`_ref_fqid`); its
    `name` is None (no live row to read). `fqid` is None only on a *malformed* triple
    (a lower-level slug-grammar concern; `_ref_fqid` mirrors `VariableRef.fqid`'s
    nullability). On the corpus today all 12 succession edges are live, but the model
    permits a dead predecessor.

    `effective_year` is the year on the `variable_replaced_by` edge that names this
    edition as the predecessor — i.e. the year this edition was superseded by its
    successor (None for the terminal, which has no outbound edge). `reason` carries
    that edge's `beskrivning` (the human transition reason) —
    UNLIKE `ClassificationEdition`, which has no reason column on its succession
    table (its edges carry `note` provenance instead); the variable grain mirrors
    how `VariableRef` carries `reason`. `is_current` marks the terminal (head)
    edition — the one with no outbound successor; `is_self` marks the edition the
    caller queried (resolved to its canonical live triple when the query was a
    `same_as` alias).

    `register` is a `BaseModel` method, so the Python attr is `register_name`
    aliased to the `register` wire/init name (#681); construct/serialize as
    `register`, read as `.register_name` (see `BindingGroupRef`)."""

    fqid: Fqid | None = Field(
        description="The edition's 3-seg binding FQID. A dead/renamed predecessor "
        "(no live variable row — tolerated by design, #355/#411) still carries a valid "
        "fqid that 301-redirects to the current edition; this is None only when the "
        "triple is malformed/unresolvable (rendered as plain text, not a link)."
    )
    provider: str = Field(description="The edition's provider slug.")
    register_name: str = Field(
        alias="register", description="The edition's register slug."
    )
    variable: str = Field(description="The edition's variable slug.")
    name: str | None = Field(
        description="The edition's display name; None for a dead/renamed predecessor "
        "with no live variable row (tolerated by design, #355/#411)."
    )
    effective_year: int | None = Field(
        description="The year on the succession edge naming this edition as the "
        "predecessor — i.e. the year it was superseded by its successor; None for "
        "the terminal (head) edition, which has no outbound edge."
    )
    reason: str | None = Field(
        description="The transition reason (the succession edge's `beskrivning`); "
        "None for the terminal (no outbound edge)."
    )
    is_current: bool = Field(
        description="True for the terminal (current) edition — no outbound successor."
    )
    is_self: bool = Field(
        description="True for the edition the caller queried (resolved to its "
        "canonical live triple when the query was a same_as alias)."
    )


class ClassificationCode(_CatalogModel):
    """One code/label entry in a classification edition's value set (#609), as
    returned by `Catalog.classification_codes`. Keyed per EDITION
    (`classification_code.classification_id`) — every edition (slug) is its own
    `classification` row, so the codes are scoped to the resolved edition the leaf
    is viewing, not the whole succession chain.

    `code`/`label` come from `value_code` (provider-native strings — these are
    PUBLIC classification codes, not row-level data). `level` is the optional
    hierarchy depth (None when the classification is flat). `is_valid` is True
    for canonical CSV-backed rows and None when no canonical CSV exists for the
    edition. Observed value-set codes that do not belong to the classification are
    state-local conformance warnings, not classification codes."""

    code: str = Field(description="The provider-native value code (e.g. '3').")
    label: str = Field(description="The human label for the code.")
    level: int | None = Field(
        description="The hierarchy depth, or None when the classification is flat."
    )
    is_valid: bool | None = Field(
        description="Canonical (True) / unknown (None — no "
        "canonical CSV exists for this edition)."
    )


class LineageEdge(_CatalogModel):
    """Consumer-side lineage at STATE grain (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage)): one `variable_state_lineage`
    row tying a consumer state to a source state over their validity
    intersection. `source_fqid` is the source state's 3-part binding FQID,
    best-effort (None when the source's slugs aren't populated)."""

    consumer_state_id: int
    source_state_id: int
    valid_from: str  # intersection start (ISO 8601 'YYYY-MM-DD')
    valid_to: str  # intersection end (ISO 8601 'YYYY-MM-DD')
    source_fqid: Fqid | None = None


class LineageWarning(_CatalogModel):
    """Build-time lineage warning for a consumer state (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage)):
    `variable_state_lineage_warning`. `warning_kind` is 'no_source_state' or
    'ambiguous_source_variant'."""

    consumer_state_id: int
    warning_kind: str
    message: str


class ResolvedVariable(_CatalogModel):
    """Longitudinal resolution of a binding FQID (see DESIGN.md → Catalog API surface): the addressable
    variable's shared metadata + its full `variable_state` history (each state
    tagged with its variant) + variable-grain edges."""

    # The caller's 3-segment binding FQID, preserved through a `same_as`
    # traversal (so a result reports the FQID the caller asked for).
    fqid: Fqid
    # The CANONICAL binding FQID of the resolved variable itself (the
    # (provider, register, slug) triple the binding resolved *to*). Equals `fqid`
    # on a direct hit; differs when `fqid` is a `same_as` alias. This is the
    # identity the edge accessors key off — graph nodes use it so an alias-entry
    # graph keys on the canonical, not the caller's address.
    canonical_fqid: Fqid
    variable_id: int
    register_id: int
    provider_key: str
    name: str | None
    definition: str | None
    description: str | None
    # SCB's "operationell definition" — per-(split-)variable distinguishing text
    # (#892/#932). Disambiguates parallel concept-group members whose only differing
    # metadata is this field (e.g. owner / previous-owner näringsgren). A first-class
    # column since schema 6.1.0, no longer folded into `description`.
    operational_definition: str | None
    measurement_unit: str | None
    is_sensitive: bool
    is_identifier: bool
    deprecated: bool = False
    source_register_id: int | None
    source_register_text: str | None
    related_documents: tuple[RelatedDocument, ...] = ()
    # Full state history, chronological ascending (oldest first). Each state
    # carries its variant coordinate + period range.
    states: tuple[VariableState, ...]
    same_as: tuple[VariableRef, ...]  # variable_same_as (equivalence)
    replaced_by: tuple[
        VariableRef, ...
    ]  # OUTBOUND successors (see DESIGN.md → Catalog API surface)
    lineage: tuple[LineageEdge, ...]  # variable_state_lineage (consumer-side)
    # The binding's owning concept group as `(provider, register, key)` when it is
    # a group member, else None (#616). Lets a member page render group-aware
    # without a second fetch. Membership is 1:1 (DB PK), so this is singular; the
    # member list lives behind `concept_group()`. Keyed on the RESOLVED variable's
    # triple (like the edges), so a same_as alias reports its target's group.
    group: BindingGroupRef | None = None
    tags: tuple[TagMembership, ...] = ()
    # Traversal path (3-segment binding FQIDs) when resolved via `same_as`; None
    # on a direct hit.
    via_same_as: tuple[Fqid, ...] | None = None


ResolvedEntity = (
    ResolvedProvider | ResolvedRegister | ResolvedVariable | ResolvedClassification
)


def _not_found(fqid: Fqid) -> RegMetaError:
    return RegMetaError(
        exit_code=EXIT_NOT_FOUND,
        code="fqid_not_found",
        error_class="query",
        message=f"FQID does not resolve to any row: {fqid!s}",
        remediation="Use `reg-meta search` to locate entities by name or ID.",
    )


# Distinguishes "variant slug names no variant under this register" (→ empty
# resolve_at result) from the `_default` slug (→ None register_variant_id, which
# matches no real state — also correct). `None` alone can't carry both meanings.
class _Missing:
    """Sentinel type for `_resolve_variant_id`. A distinct class rather than a
    bare `object()` so `ty` narrows `int | _Missing` to `int` past the guard
    (no `# type: ignore` needed at the call site)."""


_MISSING = _Missing()


def _bad_period(period: Period, detail: str) -> RegMetaError:
    return RegMetaError(
        exit_code=EXIT_USAGE,
        code="invalid_period",
        error_class="query",
        message=f"invalid period {period!r}: {detail}",
        remediation=(
            "Use an int year, a period token (HT2020 / 2020-Q3 / 2020-08 / "
            "2018-12-31), a range {'from': ..., 'to': ...}, or '_default'."
        ),
    )


def _endpoint_bounds(value: int | str) -> tuple[str, str]:
    """ISO `(lo, hi)` for a single range endpoint (int year or period token)."""
    if isinstance(value, bool):  # bool is an int subclass — reject explicitly
        raise _bad_period(value, "range endpoint must be a year or period token")
    if isinstance(value, int):
        return f"{value:04d}-01-01", f"{value:04d}-12-31"
    if isinstance(value, str):
        try:
            return period_token_to_bounds(value)
        except FqidError as exc:
            raise _bad_period(value, str(exc)) from exc
    raise _bad_period(value, "range endpoint must be a year or period token")


def _period_bounds(period: Period) -> tuple[str, str] | None:
    """Expand a `Period` (see reg_schema/DESIGN.md → Two layers: models vs. validator) to an inclusive ISO `(lo, hi)` interval, or None
    for the `_default` sentinel (no period filter). Fail-fast on malformed
    input (`invalid_period`, EXIT_USAGE)."""
    if isinstance(period, bool):  # bool is an int subclass — reject before int
        raise _bad_period(period, "expected int year, token, range, or '_default'")
    if isinstance(period, int):
        return f"{period:04d}-01-01", f"{period:04d}-12-31"
    if isinstance(period, str):
        if period == DEFAULT_VARIANT_SLUG:  # "_default" snapshot sentinel
            return None
        try:
            return period_token_to_bounds(period)
        except FqidError as exc:
            raise _bad_period(period, str(exc)) from exc
    if isinstance(period, dict):
        if set(period) != {"from", "to"}:
            raise _bad_period(period, "range must have exactly 'from' and 'to' keys")
        lo, _ = _endpoint_bounds(period["from"])
        _, hi = _endpoint_bounds(period["to"])
        if lo > hi:
            raise _bad_period(period, "range 'from' is after 'to'")
        return lo, hi
    raise _bad_period(period, "expected int year, token, range, or '_default'")


class Catalog:
    """FQID resolution against an open reg_meta SQLite connection."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        doc_conn: sqlite3.Connection | None = None,
        *,
        classification_as_of_year: int | None = None,
    ) -> None:
        self._conn = conn
        self._doc_conn = doc_conn
        self._classification_as_of_year = (
            classification_as_of_year
            if classification_as_of_year is not None
            else classification_succession_as_of_year(conn)
        )
        # Source-side keys present in `variable_same_as` / `classification_same_as`.
        # Loaded lazily on first miss; same_as graphs are curator-curated and
        # tiny (tens of entries), and the common case is "no edge for this
        # tuple", so a cached frozenset short-circuits BFS without an SQL round
        # trip every miss. Catalog treats the DB as immutable for its lifetime.
        self._var_same_as_sources: frozenset[tuple[str, str, str]] | None = None
        self._class_same_as_sources: frozenset[tuple[str, str]] | None = None
        self._variant_family_cache: dict[int, dict[str, _VariantFamilyInfo]] = {}

    @classmethod
    def open(
        cls,
        db_arg: str | Path | None = None,
        *,
        with_docs: bool = False,
        classification_as_of_year: int | None = None,
    ) -> Catalog:
        path = db_path_from_args(str(db_arg) if db_arg is not None else None)
        conn = open_db(path)
        if not with_docs:
            return cls(conn, classification_as_of_year=classification_as_of_year)
        try:
            from .doc_db import ensure_doc_db

            doc_conn = ensure_doc_db(str(db_arg) if db_arg is not None else None)
        except Exception:
            conn.close()
            raise
        return cls(
            conn,
            doc_conn=doc_conn,
            classification_as_of_year=classification_as_of_year,
        )

    def close(self) -> None:
        self._conn.close()
        if self._doc_conn is not None:
            self._doc_conn.close()

    def resolve(self, fqid: str | Fqid) -> ResolvedEntity:
        if isinstance(fqid, str):
            parsed = parse(fqid)
        else:
            # Round-trip-validate so a hand-constructed Fqid with missing
            # required fields fails fast with FqidError instead of TypeError
            # inside a resolver.
            parsed = parse(str(fqid))
            if parsed.kind is not fqid.kind:
                raise FqidError(
                    f"Fqid(kind={fqid.kind.value}) is incomplete; "
                    f"emit-then-parse yields kind={parsed.kind.value}"
                )
        return _DISPATCH[parsed.kind](self, parsed)

    # ── A5.1b-i catalog-tree children enumeration (see DESIGN.md → Catalog API surface) ──
    # The webapp browse consumes these for the `/api/catalog` node tree. Each
    # returns a thin Summary list, slug-ordered (deterministic, matches the
    # FQID-leaf the webapp links on). An unknown parent slug returns an empty
    # list — not an error: a genuinely-absent node maps to 404 via `resolve()`,
    # and a present parent with no children maps to an empty children list.

    def catalog_sizes(self) -> CatalogSizes:
        """Headline catalog-size counts for the landing page — the
        BROWSE-ADDRESSABLE (slugged) grain, so each count mirrors its `list_*`
        method's filter exactly: providers are always slugged (`list_providers`
        applies no filter); registers count only the slugged rows
        (`slug IS NOT NULL` — a NULL-slug row isn't reachable by an FQID, so the
        browse listings drop it). The variable count requires BOTH the variable
        AND its parent register to be slugged: the browse can't navigate into a
        NULL-slug register (`list_registers` drops it), so a slugged variable
        under one is unreachable and must not be counted. These are FULL-UNIVERSE
        (the whole DB) — a steward-filter-aware count (a filtered deployment
        browses fewer nodes) is a webapp concern / follow-up, NOT reg_meta's job
        (reg_meta has no notion of webapp steward filtering). Three cheap COUNT
        queries."""
        return CatalogSizes(
            providers=self._conn.execute("SELECT COUNT(*) FROM provider").fetchone()[0],
            registers=self._conn.execute(
                "SELECT COUNT(*) FROM register WHERE slug IS NOT NULL"
            ).fetchone()[0],
            variables=self._conn.execute(
                "SELECT COUNT(*) FROM variable v "
                "JOIN register r ON v.register_id = r.register_id "
                "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL"
            ).fetchone()[0],
        )

    def list_providers(self) -> list[ProviderSummary]:
        """Every provider in the catalog (e.g. scb, sos), ordered by slug."""
        rows = self._conn.execute(
            "SELECT slug, name FROM provider ORDER BY slug"
        ).fetchall()
        return [
            ProviderSummary(fqid=Fqid.provider_fqid(r["slug"]), name=r["name"])
            for r in rows
        ]

    def list_registers(self, provider_slug: str) -> list[RegisterSummary]:
        """Registers under a provider, ordered by slug. A register with a NULL
        slug isn't addressable by a register FQID, so it's excluded (symmetric
        with `list_bindings`'s variable-slug filter; `register.slug` is nullable
        like `variable.slug`, both filled by the build's slug derivation). Empty
        when the provider slug names no provider OR the provider has no slugged
        registers (both are an empty children list to the webapp)."""
        rows = self._conn.execute(
            "SELECT r.slug, r.name, r.purpose "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug IS NOT NULL ORDER BY r.slug",
            (provider_slug,),
        ).fetchall()
        return [
            RegisterSummary(
                fqid=Fqid.register_fqid(provider_slug, r["slug"]),
                name=r["name"],
                purpose=r["purpose"],
            )
            for r in rows
        ]

    def list_bindings(
        self, provider_slug: str, register_slug: str
    ) -> list[BindingSummary]:
        """A register's bindings — its register-unique variable slugs — ordered
        by slug. A variable with a NULL slug isn't addressable by a binding FQID,
        so it's excluded (the FQID leaf is the browse key). Empty when the
        (provider, register) pair names no register OR it has no slugged
        variables."""
        rows = self._conn.execute(
            "SELECT v.slug, v.name "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL "
            "ORDER BY v.slug",
            (provider_slug, register_slug),
        ).fetchall()
        return [
            BindingSummary(
                fqid=Fqid.binding_fqid(provider_slug, register_slug, r["slug"]),
                name=r["name"],
            )
            for r in rows
        ]

    def register_variable_coverage(
        self, provider_slug: str, register_slug: str
    ) -> dict[str, VariableCoverage]:
        """Per-variable coverage for a register's bindings (#351), keyed by
        variable slug (the binding-FQID leaf, so the webapp zips it onto each
        `list_bindings` child). One GROUP BY over `variable_state`; a LEFT JOIN
        keeps stateless variables (coverage None, count 0). Measured ~9 ms on the
        worst real register (scb/ulf, 7.3k variables) — query-time, no
        materialized columns (see reg_webapp/DESIGN.md → Coverage aggregates)."""
        rows = self._conn.execute(
            "SELECT v.slug AS slug, MIN(vs.valid_from) AS cov_from, "
            "MAX(vs.valid_to) AS cov_to, COUNT(vs.state_id) AS nstates "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "LEFT JOIN variable_state vs ON vs.variable_id = v.variable_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL "
            "GROUP BY v.variable_id",
            (provider_slug, register_slug),
        ).fetchall()
        out: dict[str, VariableCoverage] = {}
        for r in rows:
            cov_from, cov_to, open_ended = _coverage_bounds(r["cov_from"], r["cov_to"])
            out[r["slug"]] = VariableCoverage(
                coverage_from=cov_from,
                coverage_to=cov_to,
                open_ended=open_ended,
                state_count=r["nstates"],
            )
        return out

    def register_column_coverage(
        self, provider_slug: str, register_slug: str
    ) -> dict[tuple[str, str], VariableCoverage]:
        """Per-DELIVERY-COLUMN coverage for a register's bindings (#819), keyed by
        `(variable slug, delivery_column_name)`. The sibling of
        `register_variable_coverage`, but the GROUP BY also splits on
        `vs.delivery_column_name`, so each representation of a variable (e.g. CDISP
        vs CDISP5 on one `disponibel-inkomst` member, or DIN83/DIN84/DIN86 on `din8`)
        gets its OWN window — not the variable's union span. The webapp zips this
        onto a representation member (`delivery_column` non-None); whole-variable
        members keep falling back to `register_variable_coverage`. Rows whose
        `delivery_column_name` is NULL (a stateless variable, or a state with no
        per-column name) are skipped — they have no per-column key and are served by
        the variable-level fallback. Same query shape/cost as
        `register_variable_coverage`."""
        rows = self._conn.execute(
            "SELECT v.slug AS slug, vs.delivery_column_name AS col, "
            "MIN(vs.valid_from) AS cov_from, MAX(vs.valid_to) AS cov_to, "
            "COUNT(vs.state_id) AS nstates "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "JOIN variable_state vs ON vs.variable_id = v.variable_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL "
            "  AND vs.delivery_column_name IS NOT NULL "
            "GROUP BY v.variable_id, vs.delivery_column_name",
            (provider_slug, register_slug),
        ).fetchall()
        out: dict[tuple[str, str], VariableCoverage] = {}
        for r in rows:
            cov_from, cov_to, open_ended = _coverage_bounds(r["cov_from"], r["cov_to"])
            out[(r["slug"], r["col"])] = VariableCoverage(
                coverage_from=cov_from,
                coverage_to=cov_to,
                open_ended=open_ended,
                state_count=r["nstates"],
            )
        return out

    def register_unnamed_column_coverage(
        self, provider_slug: str, register_slug: str
    ) -> dict[str, VariableCoverage]:
        """Coverage for states whose delivery-column name is NULL, keyed by slug.

        `register_column_coverage` intentionally has no `(slug, None)` key because
        representation members are keyed by named delivery columns. Filtered steward
        coverage still needs exact coverage for a held unnamed column; using the
        variable-level union would borrow named sibling states.
        """
        rows = self._conn.execute(
            "SELECT v.slug AS slug, MIN(vs.valid_from) AS cov_from, "
            "MAX(vs.valid_to) AS cov_to, COUNT(vs.state_id) AS nstates "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "JOIN variable_state vs ON vs.variable_id = v.variable_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug IS NOT NULL "
            "  AND vs.delivery_column_name IS NULL "
            "GROUP BY v.variable_id",
            (provider_slug, register_slug),
        ).fetchall()
        out: dict[str, VariableCoverage] = {}
        for r in rows:
            cov_from, cov_to, open_ended = _coverage_bounds(r["cov_from"], r["cov_to"])
            out[r["slug"]] = VariableCoverage(
                coverage_from=cov_from,
                coverage_to=cov_to,
                open_ended=open_ended,
                state_count=r["nstates"],
            )
        return out

    def provider_register_coverage(
        self, provider_slug: str
    ) -> dict[str, RegisterCoverage]:
        """Per-register coverage for a provider's registers (#351), keyed by
        register slug. `variable_count` counts slugged variables (matching
        `list_bindings`); the span is over all their states. One GROUP BY;
        ~40 ms across scb's 238 registers (query-time — see DESIGN.md)."""
        rows = self._conn.execute(
            "SELECT r.slug AS slug, COUNT(DISTINCT v.variable_id) AS nvar, "
            "MIN(vs.valid_from) AS cov_from, MAX(vs.valid_to) AS cov_to "
            "FROM register r "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "LEFT JOIN variable v "
            "ON v.register_id = r.register_id AND v.slug IS NOT NULL "
            "LEFT JOIN variable_state vs ON vs.variable_id = v.variable_id "
            "WHERE p.slug = ? AND r.slug IS NOT NULL "
            "GROUP BY r.register_id",
            (provider_slug,),
        ).fetchall()
        out: dict[str, RegisterCoverage] = {}
        for r in rows:
            cov_from, cov_to, open_ended = _coverage_bounds(r["cov_from"], r["cov_to"])
            out[r["slug"]] = RegisterCoverage(
                variable_count=r["nvar"],
                coverage_from=cov_from,
                coverage_to=cov_to,
                open_ended=open_ended,
            )
        return out

    def list_variants(
        self, provider_slug: str, register_slug: str
    ) -> list[VariantSummary]:
        """A register's variants — the `register_variant` sub-resource (the
        `?variant=` browse axis; see reg_webapp/DESIGN.md → Catalog router structure) — ordered by slug. A variant with a NULL
        slug isn't browse-addressable, so it's excluded (symmetric with
        `list_bindings`). Empty when the (provider, register) pair names no
        register OR it has no slugged variants. (`_default` is a real variant
        slug — the synthesized variant for LSS/BU/SOL etc.; see DESIGN.md → Two-level variable model — so it is
        returned, not filtered.)"""
        rows = self._conn.execute(
            "SELECT rv.register_variant_id, rv.register_id, rv.slug, rv.name, "
            "rv.description, "
            "rv.display_group, "
            "rv.panel_entity_key, rv.panel_time_key, rv.panel_time_grain "
            "FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug IS NOT NULL "
            "ORDER BY rv.slug",
            (provider_slug, register_slug),
        ).fetchall()
        versions_by_variant = self._register_version_metadata_by_variant(
            [r["register_variant_id"] for r in rows]
        )
        family_by_variant = (
            self._variant_families_for_register_id(rows[0]["register_id"])
            if rows
            else {}
        )
        return [
            VariantSummary(
                slug=r["slug"],
                name=r["name"],
                description=r["description"],
                display_group=r["display_group"],
                variant_family=(
                    family_by_variant[r["slug"]][0]
                    if r["slug"] in family_by_variant
                    else None
                ),
                variant_family_label=(
                    family_by_variant[r["slug"]][1]
                    if r["slug"] in family_by_variant
                    else None
                ),
                panel_entity_key=_decode_panel_entity_key(r["panel_entity_key"]),
                # `_decode_panel_entity_key` is the generic stored-key decoder —
                # reused for the (now-composite-capable) time key too.
                panel_time_key=_decode_panel_entity_key(r["panel_time_key"]),
                panel_time_grain=r["panel_time_grain"],
                versions=versions_by_variant.get(r["register_variant_id"], ()),
            )
            for r in rows
        ]

    def _variant_families_for_register_id(
        self, register_id: int
    ) -> dict[str, _VariantFamilyInfo]:
        """Curated register_variant succession families for one register (#376).

        Returns `variant_slug -> (family_key, family_label)`. The family key is the
        terminal/head variant slug in that register-local succession component.
        """
        cached = self._variant_family_cache.get(register_id)
        if cached is not None:
            return cached

        variants = self._conn.execute(
            "SELECT slug, name, display_group FROM register_variant "
            "WHERE register_id = ? AND slug IS NOT NULL",
            (register_id,),
        ).fetchall()
        by_slug = {row["slug"]: row for row in variants}
        if len(by_slug) < 2:
            self._variant_family_cache[register_id] = {}
            return {}

        reg = self._conn.execute(
            "SELECT p.slug AS provider_slug, r.slug AS register_slug "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE r.register_id = ?",
            (register_id,),
        ).fetchone()
        if reg is None or reg["provider_slug"] is None or reg["register_slug"] is None:
            self._variant_family_cache[register_id] = {}
            return {}

        edge_rows = self._conn.execute(
            "SELECT predecessor_variant, successor_variant "
            "FROM variant_replaced_by "
            "WHERE predecessor_provider = ? AND predecessor_register = ? "
            "AND successor_provider = ? AND successor_register = ? "
            "ORDER BY predecessor_variant, successor_variant",
            (
                reg["provider_slug"],
                reg["register_slug"],
                reg["provider_slug"],
                reg["register_slug"],
            ),
        ).fetchall()

        outgoing: dict[str, set[str]] = {}
        undirected: dict[str, set[str]] = {}
        for row in edge_rows:
            pred = row["predecessor_variant"]
            succ = row["successor_variant"]
            if pred not in by_slug or succ not in by_slug:
                continue
            outgoing.setdefault(pred, set()).add(succ)
            undirected.setdefault(pred, set()).add(succ)
            undirected.setdefault(succ, set()).add(pred)

        out: dict[str, _VariantFamilyInfo] = {}
        seen: set[str] = set()
        for start in sorted(undirected):
            if start in seen:
                continue
            stack = [start]
            component: set[str] = set()
            while stack:
                node = stack.pop()
                if node in component:
                    continue
                component.add(node)
                stack.extend(undirected.get(node, ()))
            seen.update(component)
            if len(component) < 2:
                continue
            heads = sorted(v for v in component if not outgoing.get(v))
            family_key = heads[0] if heads else sorted(component)[0]
            labels = [
                by_slug[v]["display_group"] or by_slug[v]["name"] or v
                for v in sorted(component)
            ]
            family_label = _variant_family_label(labels)
            for variant in component:
                out[variant] = (family_key, family_label)

        self._variant_family_cache[register_id] = out
        return out

    def _variant_family_for_variant_id(
        self, register_variant_id: int
    ) -> _VariantFamilyInfo | None:
        row = self._conn.execute(
            "SELECT register_id, slug FROM register_variant WHERE register_variant_id = ?",
            (register_variant_id,),
        ).fetchone()
        if row is None or row["slug"] is None:
            return None
        return self._variant_families_for_register_id(row["register_id"]).get(
            row["slug"]
        )

    def _register_version_metadata_by_variant(
        self, register_variant_ids: list[int]
    ) -> dict[int, tuple[RegisterVersionMetadata, ...]]:
        """Version-scoped metadata for the variant browser (#799)."""
        if not register_variant_ids:
            return {}
        placeholders = ",".join("?" for _ in register_variant_ids)
        version_rows = self._conn.execute(
            "SELECT regver_id, register_variant_id, registerversionnamn, "
            "registerversionbeskrivning, registerversionmatinformation "
            "FROM register_version "
            f"WHERE register_variant_id IN ({placeholders}) "
            "ORDER BY register_variant_id, registerversionnamn, regver_id",
            register_variant_ids,
        ).fetchall()
        if not version_rows:
            return {}
        regver_ids = [r["regver_id"] for r in version_rows]
        regver_placeholders = ",".join("?" for _ in regver_ids)
        populations: dict[int, list[PopulationMetadata]] = {
            rid: [] for rid in regver_ids
        }
        for row in self._conn.execute(
            "SELECT regver_id, name, definition, comment, date_range "
            "FROM population "
            f"WHERE regver_id IN ({regver_placeholders}) "
            "ORDER BY regver_id, name",
            regver_ids,
        ).fetchall():
            if not _has_text(
                row["name"], row["definition"], row["comment"], row["date_range"]
            ):
                continue
            populations[row["regver_id"]].append(
                PopulationMetadata(
                    name=row["name"],
                    definition=row["definition"],
                    comment=row["comment"],
                    date_range=row["date_range"],
                )
            )
        object_types: dict[int, list[ObjectTypeMetadata]] = {
            rid: [] for rid in regver_ids
        }
        for row in self._conn.execute(
            "SELECT regver_id, name, definition "
            "FROM object_type "
            f"WHERE regver_id IN ({regver_placeholders}) "
            "ORDER BY regver_id, name",
            regver_ids,
        ).fetchall():
            if not _has_text(row["name"], row["definition"]):
                continue
            object_types[row["regver_id"]].append(
                ObjectTypeMetadata(
                    name=row["name"],
                    definition=row["definition"],
                )
            )
        out: dict[int, list[RegisterVersionMetadata]] = {}
        for row in version_rows:
            regver_id = row["regver_id"]
            out.setdefault(row["register_variant_id"], []).append(
                RegisterVersionMetadata(
                    name=row["registerversionnamn"],
                    description=row["registerversionbeskrivning"],
                    measurement_information=row["registerversionmatinformation"],
                    populations=tuple(populations[regver_id]),
                    object_types=tuple(object_types[regver_id]),
                )
            )
        return {variant_id: tuple(items) for variant_id, items in out.items()}

    def list_concept_groups(
        self, provider_slug: str, register_slug: str
    ) -> list[ConceptGroupSummary]:
        """Derived concept groups for a register (#303; see DESIGN.md →
        Concept groups), ordered by group key. Presentation-only: members
        carry the real binding FQIDs; browse surfaces collapse member rows
        under the group and expand to a facet picker. Empty when the
        (provider, register) pair names no register OR it has no groups."""
        # Multi-axis read (#819): a group's ordered named axes live in
        # `concept_group_axis`; a REPRESENTATION member (`concept_group_variable`,
        # surrogate `member_id` + nullable `delivery_column_name`) carries one facet
        # per axis on `concept_group_variable_facet`. A member therefore spans
        # several rows (one per facet) — the accumulator groups by member_id, orders
        # facets by axis ordinal, and carries `delivery_column_name`. LEFT JOIN so a
        # facet-less edge member (zero axes) still yields its member row. Two members
        # can share an `fqid` (one variable, two delivery columns) — they stay
        # distinct member entries, distinguished by `delivery_column`. `axis_ordinal`
        # rides each facet row (joined from `concept_group_axis`) so the assembler
        # reconstructs the group's ordered axes from these rows — no per-group query.
        rows = self._conn.execute(
            "SELECT g.group_id, g.group_key, g.label AS group_label, g.source, "
            "m.member_id, m.delivery_column_name, "
            "v.variable_id, v.slug AS variable_slug, v.name AS variable_name, "
            "f.axis AS facet_axis, f.value AS facet_value, f.label AS facet_label, "
            "a.ordinal AS axis_ordinal, a.label AS axis_label "
            "FROM concept_group g "
            "JOIN register r ON g.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "JOIN concept_group_variable m ON m.group_id = g.group_id "
            "JOIN variable v ON v.variable_id = m.variable_id "
            "LEFT JOIN concept_group_variable_facet f ON f.member_id = m.member_id "
            "LEFT JOIN concept_group_axis a "
            "  ON a.group_id = g.group_id AND a.axis = f.axis "
            "WHERE p.slug = ? AND r.slug = ? AND g.kind = 'variable' "
            "  AND v.slug IS NOT NULL "
            "ORDER BY g.group_key, m.member_id, a.ordinal",
            (provider_slug, register_slug),
        ).fetchall()
        groups = self._assemble_variable_groups(provider_slug, register_slug, rows)
        return sorted(groups, key=lambda g: g.key)

    def _tags_for_variable_ids(
        self, variable_ids: Iterable[int]
    ) -> tuple[TagMembership, ...]:
        ids = tuple(sorted(set(variable_ids)))
        if not ids:
            return ()
        placeholders = ",".join("?" for _ in ids)
        rows = self._conn.execute(
            "SELECT DISTINCT t.slug, t.label, tm.rank, tm.starred, tm.note, "
            "tm.variable_id AS member_variable_id "
            "FROM tag_member tm "
            "JOIN tag t ON t.tag_id = tm.tag_id "
            f"WHERE tm.variable_id IN ({placeholders}) "
            "ORDER BY tm.rank, t.slug, tm.variable_id",
            ids,
        ).fetchall()
        return _aggregate_tag_memberships(rows)

    def _assemble_variable_groups(
        self,
        provider_slug: str,
        register_slug: str,
        rows: list[sqlite3.Row],
    ) -> list[ConceptGroupSummary]:
        """Fold the flat member×facet rows of `list_concept_groups` into
        `ConceptGroupSummary` objects (#819). One member spans N facet rows (one per
        axis, plus a single NULL-facet row for a facet-less edge member). A group's
        ordered axes are folded out of the same rows: each facet row carries its
        axis's `axis_ordinal`, and the validator guarantees every declared axis is
        covered by every member's facets (one facet per axis), so the in-row
        `(ordinal, axis)` pairs reproduce `concept_group_axis ORDER BY ordinal`
        without a per-group query."""
        # group_id → (key, label, source, members-by-id, axes-by-ordinal)
        acc: dict[int, tuple[str, str, str, dict[int, dict], dict[int, GroupAxis]]] = {}
        for r in rows:
            _, _, _, members, group_axes = acc.setdefault(
                r["group_id"],
                (r["group_key"], r["group_label"], r["source"], {}, {}),
            )
            member = members.setdefault(
                r["member_id"],
                {
                    "variable_id": r["variable_id"],
                    "fqid": Fqid.binding_fqid(
                        provider_slug, register_slug, r["variable_slug"]
                    ),
                    "name": r["variable_name"],
                    "delivery_column": r["delivery_column_name"],
                    "facets": [],
                },
            )
            if r["facet_axis"] is not None:
                member["facets"].append(
                    GroupFacet(
                        axis=r["facet_axis"],
                        value=r["facet_value"],
                        label=r["facet_label"],
                    )
                )
                group_axes[r["axis_ordinal"]] = GroupAxis(
                    name=r["facet_axis"], label=r["axis_label"]
                )
        out: list[ConceptGroupSummary] = []
        for key, label, source, members, group_axes in acc.values():
            axes = tuple(group_axes[ordinal] for ordinal in sorted(group_axes))
            # Order members by their first-axis facet value, then slug, then
            # delivery column — preserving the old single-axis ordering (month 01,
            # 02 …) and giving multi-axis members a deterministic order. A facet-less
            # edge member sorts on its slug alone.
            ordered_members = sorted(
                members.values(),
                key=lambda m: (
                    m["facets"][0].value if m["facets"] else "",
                    str(m["fqid"]),
                    m["delivery_column"] or "",
                ),
            )
            out.append(
                ConceptGroupSummary(
                    key=key,
                    label=label,
                    source=_group_source(source),
                    axes=axes,
                    members=tuple(
                        ConceptGroupMember(
                            fqid=m["fqid"],
                            name=m["name"],
                            facets=tuple(m["facets"]),
                            delivery_column=m["delivery_column"],
                        )
                        for m in ordered_members
                    ),
                    tags=self._tags_for_variable_ids(
                        m["variable_id"] for m in members.values()
                    ),
                )
            )
        return out

    def concept_group(
        self, provider_slug: str, register_slug: str, key: str
    ) -> ConceptGroupSummary | None:
        """The one derived concept group addressed by `(provider, register, key)`
        (#616) — the group's scope-unique derivation key (`ConceptGroupSummary.key`,
        unique per `(provider, register)` and present for all sources). A group's
        default selection is "all members", which a member FQID can't express, so a
        group needs its own address; this is the by-key accessor for it.

        None when no group with that key exists for the pair (and when the pair
        names no register) — mirroring `list_concept_groups`' tolerance of an
        unknown pair (`[]`). Filters the register's group list (a handful of groups
        per register; reuses the member-hydration SQL)."""
        for g in self.list_concept_groups(provider_slug, register_slug):
            if g.key == key:
                return g
        return None

    def list_classification_groups(self) -> list[ConceptGroupSummary]:
        """Curated classification umbrella groups (see DESIGN.md → Concept
        groups), ordered by group key. `concept_group_classification` holds
        only CURATED umbrella entries — e.g. `group:sun`, which groups the
        three genuinely-distinct SUN dimensions (`sun2020-niva`,
        `sun2020-inriktning`, `sun2020-grupp`) plus its nivå aggregates (#516).
        Derived classification VINTAGE editions (lkf1980…lkf2026,
        ssyk1996→ssyk2012, sun2000-niva→sun2020-niva) are NOT here; they
        appear as succession edges in `classification_replaced_by` (#571).
        Members carry the real `class/<slug>` FQIDs.

        These umbrellas are AXIS-LESS: the members are distinct classifications,
        not points on a shared scale, so the group has zero `concept_group_axis`
        rows (#819) and `axes` is the empty tuple (the webapp renders the
        member-noun as "members"). Each member STILL carries its curated short facet
        `value`/`label` (the picker label) INLINE on `concept_group_classification`
        regardless of the absent group axis — so a `GroupFacet` with `axis=None` is
        emitted per member. (A group that DOES declare an axis — one
        `concept_group_axis` row — is read through with that axis name.)"""
        # The single axis declaration now lives in `concept_group_axis` (#819,
        # replacing the dropped `concept_group.facet_axis`). These umbrellas carry
        # at most one axis row, so a LEFT JOIN yields the axis name or NULL.
        rows = self._conn.execute(
            "SELECT g.group_id, g.group_key, g.label AS group_label, g.source, "
            "a.axis AS axis, a.label AS axis_label, "
            "c.slug AS cls_slug, c.name AS cls_name, m.facet_value, m.facet_label "
            "FROM concept_group g "
            "LEFT JOIN concept_group_axis a ON a.group_id = g.group_id "
            "JOIN concept_group_classification m ON m.group_id = g.group_id "
            "JOIN classification c ON c.id = m.classification_id "
            "WHERE g.kind = 'classification' AND c.slug IS NOT NULL "
            "ORDER BY g.group_key, m.facet_value, c.slug"
        ).fetchall()
        # A declared axis (NULL on the axis-less umbrellas) becomes the group's
        # single `GroupAxis(name, label)`; the per-member facet still keys on the
        # axis NAME (None when axis-less).
        acc2: dict[
            int, tuple[str, str, str, GroupAxis | None, list[ConceptGroupMember]]
        ] = {}
        for r in rows:
            axis = (
                GroupAxis(name=r["axis"], label=r["axis_label"])
                if r["axis"] is not None
                else None
            )
            _, _, _, axis, members = acc2.setdefault(
                r["group_id"],
                (r["group_key"], r["group_label"], r["source"], axis, []),
            )
            members.append(
                ConceptGroupMember(
                    fqid=Fqid.classification_fqid(r["cls_slug"]),
                    name=r["cls_name"],
                    facets=(
                        GroupFacet(
                            axis=axis.name if axis is not None else None,
                            value=r["facet_value"],
                            label=r["facet_label"],
                        ),
                    ),
                )
            )
        return [
            ConceptGroupSummary(
                key=key,
                label=label,
                source=_group_source(source),
                axes=(axis,) if axis is not None else (),
                members=tuple(members),
            )
            for key, label, source, axis, members in sorted(
                acc2.values(), key=lambda g: g[0]
            )
        ]

    def classification_group(self, key: str) -> ConceptGroupSummary | None:
        """The one curated classification umbrella group addressed by its
        derivation `key` (#761) — a filter over `list_classification_groups()`, the
        classification dual of `concept_group(provider, register, key)`. Classification
        umbrellas are catalog-global (`register_id NULL`), so the key alone addresses
        them (no provider/register scope). None when no umbrella has that key.

        #756 did the same class-by-key filter INLINE in the webapp to avoid a
        reg_meta release; #761 ships a reg_meta release anyway, so the accessor
        lands here (and the webapp's `get_classification_group` can dedupe onto it —
        an optional follow-on)."""
        for g in self.list_classification_groups():
            if g.key == key:
                return g
        return None

    def list_classification_families(self) -> list[ClassificationFamilySummary]:
        """Derived one-dimensional classification succession families (#771).

        This is deliberately outside `concept_group`: a family key is browse identity
        over `classification_replaced_by`, not group membership. Only known
        one-dimensional families get stable route keys; multi-dimensional umbrellas
        such as SUN stay on the curated classification-group surface.
        """
        rows = self._conn.execute(
            "SELECT predecessor_slug, successor_slug "
            "FROM classification_replaced_by "
            "ORDER BY predecessor_slug, successor_slug"
        ).fetchall()
        parent: dict[str, str] = {}
        successors: set[str] = set()

        def find(slug: str) -> str:
            parent.setdefault(slug, slug)
            while parent[slug] != slug:
                parent[slug] = parent[parent[slug]]
                slug = parent[slug]
            return slug

        def union(left: str, right: str) -> None:
            left_root = find(left)
            right_root = find(right)
            if left_root != right_root:
                parent[right_root] = left_root

        for row in rows:
            predecessor = row["predecessor_slug"]
            successor = row["successor_slug"]
            successors.add(successor)
            union(predecessor, successor)

        components: dict[str, set[str]] = {}
        for slug in list(parent):
            components.setdefault(find(slug), set()).add(slug)

        slugs_by_key: dict[str, set[str]] = {}
        for slugs in components.values():
            keys = {
                key
                for slug in slugs
                if (key := _classification_family_key(slug)) is not None
            }
            if keys:
                key = sorted(keys)[0]
                slugs_by_key.setdefault(key, set()).update(slugs)

        families: list[ClassificationFamilySummary] = []
        for key, slugs in sorted(slugs_by_key.items()):
            roots = sorted(slug for slug in slugs if slug not in successors)
            anchors = roots or sorted(slugs)
            editions: list[ClassificationEdition] = []
            seen: set[str] = set()
            for anchor in anchors:
                for edition in self.classification_chain(
                    Fqid.classification_fqid(anchor)
                ):
                    if edition.slug in seen:
                        continue
                    if _classification_family_key(edition.slug) != key:
                        continue
                    seen.add(edition.slug)
                    editions.append(edition)
            if editions:
                families.append(
                    ClassificationFamilySummary(
                        key=key,
                        label=_CLASSIFICATION_FAMILY_LABELS[key],
                        editions=tuple(editions),
                    )
                )
        return families

    def classification_family(self, key: str) -> ClassificationFamilySummary | None:
        """The one derived classification succession family addressed by `key`."""
        for family in self.list_classification_families():
            if family.key == key:
                return family
        return None

    def list_tags(self) -> list[TagSummary]:
        """The curated thematic tag vocabulary (#311) with per-tag member counts,
        ordered by slug. `member_count` spans both grains; `starred_count` is the
        golden/recommended subset. Empty when no tags are curated (the machinery-
        only ship state)."""
        rows = self._conn.execute(
            "SELECT t.slug, t.label, t.description, "
            "COUNT(tm.tag_id) AS member_count, "
            "COALESCE(SUM(tm.starred), 0) AS starred_count "
            "FROM tag t "
            "LEFT JOIN tag_member tm ON tm.tag_id = t.tag_id "
            "GROUP BY t.tag_id "
            "ORDER BY t.slug"
        ).fetchall()
        return [
            TagSummary(
                slug=r["slug"],
                label=r["label"],
                description=r["description"],
                member_count=r["member_count"],
                starred_count=r["starred_count"],
            )
            for r in rows
        ]

    def _direct_tags_for_variable(self, fqid: Fqid) -> list[TagMembership]:
        rows = self._conn.execute(
            "SELECT t.slug, t.label, tm.rank, tm.starred, tm.note "
            "FROM tag_member tm "
            "JOIN tag t ON t.tag_id = tm.tag_id "
            "JOIN variable v ON v.variable_id = tm.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug = ? "
            "ORDER BY tm.rank, t.slug",
            (fqid.provider, fqid.register, fqid.variable),
        ).fetchall()
        return [_tag_membership(r) for r in rows]

    def _group_tags_for_variable(self, fqid: Fqid) -> tuple[TagMembership, ...]:
        rows = self._conn.execute(
            "SELECT DISTINCT t.slug, t.label, tm.rank, tm.starred, tm.note, "
            "tm.variable_id AS member_variable_id "
            "FROM variable target "
            "JOIN register target_r ON target.register_id = target_r.register_id "
            "JOIN provider target_p ON target_r.provider_id = target_p.provider_id "
            "JOIN concept_group_variable target_member "
            "  ON target_member.variable_id = target.variable_id "
            "JOIN concept_group_variable group_member "
            "  ON group_member.group_id = target_member.group_id "
            "JOIN tag_member tm ON tm.variable_id = group_member.variable_id "
            "JOIN tag t ON t.tag_id = tm.tag_id "
            "WHERE target_p.slug = ? AND target_r.slug = ? AND target.slug = ? "
            "ORDER BY tm.rank, t.slug, tm.variable_id",
            (fqid.provider, fqid.register, fqid.variable),
        ).fetchall()
        return _aggregate_tag_memberships(rows)

    def tags_for_variable(self, fqid: Fqid) -> list[TagMembership]:
        """Tags the variable at `fqid` (a 3-seg binding FQID) belongs to (#311),
        ordered by tag rank then slug.

        Direct variable-grain memberships keep their curated rank/starred/note.
        If the variable is in a concept group, thematic tags curated on any sibling
        member are inherited as neutral memberships so every member shares the
        group-level theme without copying a representative member's note/star.
        """
        direct = self._direct_tags_for_variable(fqid)
        direct_slugs = {tag.slug for tag in direct}
        inherited = [
            TagMembership(
                slug=tag.slug,
                label=tag.label,
                rank=tag.rank,
                starred=False,
                note=None,
            )
            for tag in self._group_tags_for_variable(fqid)
            if tag.slug not in direct_slugs
        ]
        return sorted([*direct, *inherited], key=lambda tag: (tag.rank, tag.slug))

    def tags_for_register(self, fqid: Fqid) -> list[TagMembership]:
        """Tags the register at `fqid` (a 2-seg `provider/register` FQID) belongs
        to (#311), ordered by tag rank then slug. Empty when none (or the FQID
        names no register)."""
        rows = self._conn.execute(
            "SELECT t.slug, t.label, tm.rank, tm.starred, tm.note "
            "FROM tag_member tm "
            "JOIN tag t ON t.tag_id = tm.tag_id "
            "JOIN register r ON r.register_id = tm.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? "
            "ORDER BY tm.rank, t.slug",
            (fqid.provider, fqid.register),
        ).fetchall()
        return [_tag_membership(r) for r in rows]

    def _resolve_provider(self, fqid: Fqid) -> ResolvedProvider:
        row = self._conn.execute(
            "SELECT provider_id, name FROM provider WHERE slug = ?",
            (fqid.provider,),
        ).fetchone()
        if not row:
            raise _not_found(fqid)
        return ResolvedProvider(
            fqid=fqid, provider_id=row["provider_id"], name=row["name"]
        )

    def _resolve_register(self, fqid: Fqid) -> ResolvedRegister:
        # Glossary rename (see DESIGN.md → Glossary and Swedish↔English crosswalk): `name` / `purpose` (was `registernamn` / `registersyfte`).
        # `registerrubrik` is dropped per the same glossary rename.
        row = self._conn.execute(
            "SELECT r.register_id, r.provider_id, r.name, r.purpose "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ?",
            (fqid.provider, fqid.register),
        ).fetchone()
        if not row:
            raise _not_found(fqid)
        return ResolvedRegister(
            fqid=fqid,
            register_id=row["register_id"],
            provider_id=row["provider_id"],
            name=row["name"],
            purpose=row["purpose"],
            related_documents=self._related_documents_for_register(fqid.register),
            tags=tuple(self.tags_for_register(fqid)),
        )

    def _resolve_binding(self, fqid: Fqid) -> ResolvedVariable:
        """Longitudinal resolution (see DESIGN.md → Catalog API surface). The 3-segment binding FQID selects
        ONE `variable` row by register-unique slug (exact match, no
        derive-at-resolve); from it we gather the shared metadata, the full
        `variable_state` history (each tagged with its variant), and the
        variable-grain edges. Period-independent — period narrowing lives in
        `resolve_at`.
        """
        resolved = self._resolve_variable_identity(fqid)
        if resolved is None:
            raise _not_found(fqid)
        var, via_same_as = resolved
        return self._build_resolved_variable(fqid, var, via_same_as)

    def _var_same_as_source_keys(self) -> frozenset[tuple[str, str, str]]:
        if self._var_same_as_sources is None:
            rows = self._conn.execute(
                "SELECT DISTINCT a_provider, a_register, a_variable "
                "FROM variable_same_as"
            ).fetchall()
            self._var_same_as_sources = frozenset((r[0], r[1], r[2]) for r in rows)
        return self._var_same_as_sources

    def _resolve_variable_identity(
        self, fqid: Fqid
    ) -> tuple[sqlite3.Row, tuple[Fqid, ...] | None] | None:
        """Resolve a binding FQID to its `variable` row, following curated
        `variable_same_as` edges when the direct slug lookup misses. Returns
        `(variable_row, via_same_as_path)` — `via_same_as` is None on a direct
        hit, else the traversal path (3-segment binding FQIDs). None when neither
        the direct lookup nor any same_as edge resolves.

        Variable identity is period- and variant-independent (the FQID's slug is
        the register-unique natural key; see DESIGN.md → Two-level variable model) — there is no edition/instance to
        thread."""
        assert fqid.provider is not None and fqid.register is not None
        assert fqid.variable is not None
        direct = self._lookup_variable(fqid.provider, fqid.register, fqid.variable)
        if direct is not None:
            return direct, None
        return self._resolve_variable_via_same_as(fqid)

    def _resolve_variable_via_same_as(
        self, fqid: Fqid
    ) -> tuple[sqlite3.Row, tuple[Fqid, ...]] | None:
        """BFS through `variable_same_as` (variable grain; see DESIGN.md → Composite registers and source tracking) until a target
        variable EXISTS (by register-unique slug). The edge is variable grain
        (cross-register / cross-provider equivalence), so the target register
        may use other variant slugs; the path records each hop as its 3-segment
        binding FQID."""
        assert fqid.provider and fqid.register and fqid.variable
        if (
            fqid.provider,
            fqid.register,
            fqid.variable,
        ) not in self._var_same_as_source_keys():
            return None
        start_key = (fqid.provider, fqid.register, fqid.variable)
        visited: set[tuple[str, str, str]] = {start_key}
        queue: deque[tuple[str, str, str, tuple[Fqid, ...]]] = deque()
        queue.append((*start_key, ()))
        while queue:
            prov, reg, variable, path = queue.popleft()
            rows = self._conn.execute(
                "SELECT b_provider, b_register, b_variable "
                "FROM variable_same_as "
                "WHERE a_provider = ? AND a_register = ? AND a_variable = ?",
                (prov, reg, variable),
            ).fetchall()
            for row in rows:
                n_prov = row["b_provider"]
                n_reg = row["b_register"]
                n_variable = row["b_variable"]
                key = (n_prov, n_reg, n_variable)
                if key in visited:
                    continue
                visited.add(key)
                try:
                    step_fqid = Fqid.binding_fqid(n_prov, n_reg, n_variable)
                except FqidError:
                    # Malformed slug — populate_slugs validates on write, so
                    # this is a build-invariant break; skip the candidate.
                    continue
                new_path = (*path, step_fqid)
                target = self._lookup_variable(n_prov, n_reg, n_variable)
                if target is not None:
                    return target, new_path
                queue.append((n_prov, n_reg, n_variable, new_path))
        return None

    def _build_resolved_variable(
        self, fqid: Fqid, var: sqlite3.Row, via_same_as: tuple[Fqid, ...] | None
    ) -> ResolvedVariable:
        """Assemble a `ResolvedVariable` from a resolved `variable` row: shared
        metadata + full chronological state history + variable-grain edges. The
        edge accessors all key off the resolved variable's own
        (provider, register, slug) triple, so a same_as-resolved binding reports
        the TARGET's edges (the binding resolved *to* that variable)."""
        meta = self._lookup_variable_meta(var["variable_id"])
        triple = (meta["provider_slug"], meta["register_slug"], meta["slug"])
        edges = self._edges_for_variable(*triple, var["variable_id"])
        group = self._group_ref_for_variable(
            var["variable_id"], meta["provider_slug"], meta["register_slug"]
        )
        canonical_fqid = Fqid.binding_fqid(*triple)
        return ResolvedVariable(
            fqid=fqid,
            canonical_fqid=canonical_fqid,
            variable_id=var["variable_id"],
            register_id=meta["register_id"],
            provider_key=meta["provider_key"],
            name=meta["name"],
            definition=meta["definition"],
            description=meta["description"],
            operational_definition=meta["operational_definition"],
            measurement_unit=meta["measurement_unit"],
            is_sensitive=bool(meta["is_sensitive"]),
            is_identifier=bool(meta["is_identifier"]),
            deprecated=bool(meta["deprecated"]),
            source_register_id=meta["source_register_id"],
            source_register_text=meta["source_register_text"],
            related_documents=self._related_documents_for_register(
                meta["register_slug"]
            ),
            states=self._states_for_variable(var["variable_id"]),
            same_as=edges["same_as"],
            replaced_by=edges["replaced_by"],
            lineage=edges["lineage"],
            group=group,
            tags=tuple(self.tags_for_variable(canonical_fqid)),
            via_same_as=via_same_as,
        )

    def _related_documents_for_register(
        self, register_slug: str | None
    ) -> tuple[RelatedDocument, ...]:
        if self._doc_conn is None or register_slug is None:
            return ()
        from .doc_queries import related_documents_for_register

        return related_documents_for_register(self._doc_conn, register_slug)

    def _group_ref_for_variable(
        self, variable_id: int, provider_slug: str, register_slug: str
    ) -> BindingGroupRef | None:
        """The binding's owning concept group as a `(provider, register, key)`
        ref, or None when it is not a group member (#616). A variable belongs to at
        most one group (validated, #819), but a multi-axis family can carry it under
        several REPRESENTATION members, so the surrogate-keyed
        `concept_group_variable` returns N rows for one variable — `SELECT DISTINCT`
        collapses them to the single owning group_key (the singular
        `ResolvedVariable.group`). The full member list lives behind
        `concept_group()`."""
        row = self._conn.execute(
            "SELECT DISTINCT g.group_key FROM concept_group_variable m "
            "JOIN concept_group g ON g.group_id = m.group_id "
            "WHERE m.variable_id = ? AND g.kind = 'variable'",
            (variable_id,),
        ).fetchone()
        if row is None:
            return None
        return BindingGroupRef(
            provider=provider_slug, register=register_slug, key=row["group_key"]
        )

    # ── A2.5 state-anchored resolution helpers ─────────────────────────────

    def _lookup_variable(
        self, provider: str, register: str, variable_slug: str
    ) -> sqlite3.Row | None:
        """THE variable row for a register-unique slug (see DESIGN.md → Two-level variable model — natural key)."""
        return self._conn.execute(
            "SELECT v.variable_id, v.register_id, v.provider_key, "
            "v.name AS variable_name "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
            (provider, register, variable_slug),
        ).fetchone()

    def _lookup_variable_meta(self, variable_id: int) -> sqlite3.Row:
        """The variable's shared metadata + its own (provider, register, slug)
        triple — the canonical identity for the edge accessors. The variable_id
        is already resolved, so this always finds a row."""
        row = self._conn.execute(
            "SELECT v.register_id, v.provider_key, v.slug, v.name, v.definition, "
            "v.description, v.operational_definition, "
            "v.measurement_unit, v.is_sensitive, v.is_identifier, v.deprecated, "
            "v.source_register_id, v.source_register_text, "
            "r.slug AS register_slug, p.slug AS provider_slug "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE v.variable_id = ?",
            (variable_id,),
        ).fetchone()
        assert row is not None  # variable_id resolved upstream
        return row

    def _states_in_bounds(
        self,
        variable_id: int,
        register_variant_id: int | None,
        bounds: tuple[str, str] | None,
    ) -> list[sqlite3.Row]:
        """`variable_state` rows for the variable whose validity range intersects
        `bounds` (an inclusive ISO `(lo, hi)` date interval), **chronological
        ascending** (oldest first) for the public surface. `register_variant_id`
        None spans every variant; `bounds` None returns every state (the
        `_default` / no-period-filter case).

        A2.5 generalizes the interim year-granular overlap test: bounds
        are full ISO dates, so sub-annual queries (`HT2020`, `2020-08`, a range)
        intersect precisely against the stored full-date validity ranges (see DESIGN.md → Two-level variable model) —
        the year-only INTERIM limit is lifted. The interval test is the standard
        `valid_from <= hi AND valid_to >= lo` (string compare is chronologically
        correct because every stored value is a full date)."""
        # JOIN `variable` to denormalize the variable-grain `is_identifier` flag
        # onto each state (see DESIGN.md → Two-level variable model — the column is variable-grain); LEFT JOIN
        # `classification` for the per-state `classification_slug` (NULL for
        # code-less states). Columns are qualified so the ORDER BY stays
        # unambiguous.
        if register_variant_id is not None:
            rows = self._conn.execute(
                "SELECT vs.state_id, vs.register_variant_id, vs.data_type, "
                "vs.data_length, vs.delivery_column_name, vs.source_register_text, "
                "vs.operational_definition, vs.value_set_id, "
                "vs.value_set_version_label, vs.valid_from, vs.valid_to, "
                "v.is_identifier, c.slug AS classification_slug, "
                "ccf.status AS conformance_status, "
                "ccf.checked_code_count, ccf.matched_code_count, "
                "ccf.nonconforming_code_count, ccf.overlap, "
                "dc.slug AS declared_classification_slug, "
                "dc.short_name AS declared_classification_short_name, "
                "dc.name AS declared_classification_name, "
                "rv.name AS variant_label "
                "FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "JOIN register_variant rv "
                "ON vs.register_variant_id = rv.register_variant_id "
                "LEFT JOIN classification c ON vs.classification_id = c.id "
                "LEFT JOIN classification_conformance ccf ON ccf.state_id = vs.state_id "
                "LEFT JOIN classification dc ON dc.id = ccf.declared_classification_id "
                "WHERE vs.variable_id = ? AND vs.register_variant_id = ? "
                "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, "
                "vs.state_id",
                (variable_id, register_variant_id),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT vs.state_id, vs.register_variant_id, vs.data_type, "
                "vs.data_length, vs.delivery_column_name, vs.source_register_text, "
                "vs.operational_definition, vs.value_set_id, "
                "vs.value_set_version_label, vs.valid_from, vs.valid_to, "
                "v.is_identifier, c.slug AS classification_slug, "
                "ccf.status AS conformance_status, "
                "ccf.checked_code_count, ccf.matched_code_count, "
                "ccf.nonconforming_code_count, ccf.overlap, "
                "dc.slug AS declared_classification_slug, "
                "dc.short_name AS declared_classification_short_name, "
                "dc.name AS declared_classification_name, "
                "rv.name AS variant_label "
                "FROM variable_state vs "
                "JOIN variable v ON vs.variable_id = v.variable_id "
                "JOIN register_variant rv "
                "ON vs.register_variant_id = rv.register_variant_id "
                "LEFT JOIN classification c ON vs.classification_id = c.id "
                "LEFT JOIN classification_conformance ccf ON ccf.state_id = vs.state_id "
                "LEFT JOIN classification dc ON dc.id = ccf.declared_classification_id "
                "WHERE vs.variable_id = ? "
                "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, "
                "vs.register_variant_id, vs.state_id",
                (variable_id,),
            ).fetchall()
        if bounds is None:
            return rows
        lo, hi = bounds
        return [r for r in rows if r["valid_from"] <= hi and r["valid_to"] >= lo]

    def _value_set_codes(
        self, value_set_id: int | None
    ) -> tuple[ValueSetMember, ...] | None:
        """`ValueSetMember` (code, label) entries for a `value_set_id`,
        deterministically ordered. None when the state carries no value set."""
        if value_set_id is None:
            return None
        rows = self._conn.execute(
            "SELECT vc.code, vc.label FROM value_set_member vsm "
            "JOIN value_code vc ON vsm.code_id = vc.code_id "
            "WHERE vsm.value_set_id = ? ORDER BY vc.code, vc.label",
            (value_set_id,),
        ).fetchall()
        return tuple(ValueSetMember(code=r["code"], label=r["label"]) for r in rows)

    def _classification_conformance_for_state(
        self, row: sqlite3.Row
    ) -> ClassificationConformance | None:
        """Hydrate the state-local classification conformance warning, if any."""
        if row["conformance_status"] is None:
            return None
        if row["nonconforming_code_count"] == 0:
            code_rows = ()
        else:
            code_rows = self._conn.execute(
                "SELECT vc.code, vc.label "
                "FROM classification_conformance_code ccc "
                "JOIN value_code vc ON vc.code_id = ccc.code_id "
                "WHERE ccc.state_id = ? "
                "ORDER BY vc.code, vc.label",
                (row["state_id"],),
            ).fetchall()
        return ClassificationConformance(
            declared_classification_slug=row["declared_classification_slug"],
            declared_classification_short_name=row[
                "declared_classification_short_name"
            ],
            declared_classification_name=row["declared_classification_name"],
            status=row["conformance_status"],
            checked_code_count=row["checked_code_count"],
            matched_code_count=row["matched_code_count"],
            nonconforming_code_count=row["nonconforming_code_count"],
            overlap=row["overlap"],
            nonconforming_codes=tuple(
                ValueSetMember(code=r["code"], label=r["label"]) for r in code_rows
            ),
        )

    @staticmethod
    def _period_token_for_window(valid_from: str, valid_to: str) -> str | None:
        """The coarsest exact display token for a `(valid_from, valid_to)` window
        (#321/#681), or None for the open-ended sentinel (`9999-12-31` — no finite
        token). Single source for `VariableState.period_token` — populated on the
        annual row in `_row_to_state` and re-derived per month-window in
        `_expand_state_windows` (the window's bounds, not the annual base's)."""
        if valid_to == OPEN_ENDED_VALID_TO:
            return None
        return period_token_for_bounds(valid_from, valid_to)

    def _row_to_state(self, row: sqlite3.Row) -> VariableState:
        """Build a `VariableState` from a `variable_state` row, tagging it with
        its variant slug and hydrating its value set."""
        rvid = row["register_variant_id"]
        variant = self._variant_slug(rvid)
        if variant is None:
            # see DESIGN.md → Two-level variable model: register_variant_id is NOT NULL on variable_state and FK'd to
            # register_variant, so a missing slug is a build-invariant break, not
            # a normal case — surface it loudly rather than emit a bad coordinate.
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="state_variant_unresolved",
                error_class="query",
                message=(
                    f"variable_state {row['state_id']} references "
                    f"register_variant {rvid} with no slug"
                ),
                remediation="Rebuild the reg_meta DB (slug population is incomplete).",
            )
        family = self._variant_family_for_variant_id(rvid)
        return VariableState(
            state_id=row["state_id"],
            variant=variant,
            variant_label=row["variant_label"],
            variant_family=family[0] if family is not None else None,
            variant_family_label=family[1] if family is not None else None,
            register_variant_id=rvid,
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
            data_type=row["data_type"],
            data_length=row["data_length"],
            delivery_column_name=row["delivery_column_name"],
            source_register_text=row["source_register_text"],
            operational_definition=row["operational_definition"],
            value_set_version_label=row["value_set_version_label"],
            value_set_id=row["value_set_id"],
            value_set=self._value_set_codes(row["value_set_id"]),
            is_identifier=bool(row["is_identifier"]),
            classification_slug=row["classification_slug"],
            classification_conformance=self._classification_conformance_for_state(row),
            period_token=self._period_token_for_window(
                row["valid_from"], row["valid_to"]
            ),
        )

    def _states_for_variable(self, variable_id: int) -> tuple[VariableState, ...]:
        """Full chronological state history for a variable (all variants)."""
        return tuple(
            self._expand_state_windows(
                variable_id, self._states_in_bounds(variable_id, None, None), None
            )
        )

    def _variable_windows(
        self, variable_id: int
    ) -> dict[int, list[tuple[str, str, str]]]:
        """`variable_alias_window` rows (#319/#945) grouped by
        `register_variant_id` → [(delivery_column_name, valid_from, valid_to), …]
        sorted by window start. EMPTY for variables with no resolver-visible alias
        representations, so expansion is a no-op there. One indexed point-lookup
        on `idx_variable_alias_window_lookup`."""
        out: dict[int, list[tuple[str, str, str]]] = {}
        for rvid, col, wfrom, wto in self._conn.execute(
            "SELECT register_variant_id, delivery_column_name, valid_from, valid_to "
            "FROM variable_alias_window WHERE variable_id = ? "
            "ORDER BY register_variant_id, valid_from, delivery_column_name",
            (variable_id,),
        ):
            out.setdefault(rvid, []).append((col, wfrom, wto))
        return out

    def _expand_state_windows(
        self,
        variable_id: int,
        rows: list[sqlite3.Row],
        bounds: tuple[str, str] | None,
    ) -> list[VariableState]:
        """Map period-filtered `variable_state` rows to `VariableState`s, expanding
        stored alias windows that overlap `bounds`.

        Monthly-family variables (#319) expand one annual state into month-column
        windows. Multi-alias cvids (#945) expand one state into co-delivered alias
        columns over that same state window. Variables with no window rows map 1:1
        via `_row_to_state` (byte-identical behaviour). Windows share the base
        state's `state_id` + `value_set_version_label`; only
        `delivery_column_name` + `valid_from`/`valid_to` are overridden. The
        per-window identity is the compound (state_id, delivery_column_name,
        valid_from)."""
        windows_by_variant = self._variable_windows(variable_id)
        if not windows_by_variant:
            return [self._row_to_state(r) for r in rows]
        lo, hi = bounds if bounds is not None else ("0001-01-01", "9999-12-31")
        out: list[VariableState] = []
        for row in rows:
            base = self._row_to_state(row)
            windows = windows_by_variant.get(row["register_variant_id"], [])
            # Windows belonging to THIS state. Overlapping states can exist; a
            # state expands only if its own representative column participates
            # somewhere in its window set, so unrelated narrower windows do not
            # hide this state's base column.
            state_windows = [
                (col, wfrom, wto)
                for (col, wfrom, wto) in windows
                if base.valid_from <= wfrom and wto <= base.valid_to
            ]
            has_base_window = base.delivery_column_name is not None and any(
                col.lower() == base.delivery_column_name.lower()
                for (col, _wfrom, _wto) in state_windows
            )
            if state_windows and not has_base_window:
                out.append(base)
                continue
            matched = [
                (col, wfrom, wto)
                for (col, wfrom, wto) in state_windows
                if wfrom <= hi and wto >= lo
            ]
            if not matched:
                # A windowed variable's state with no window in range (e.g. a year
                # a monthly family didn't deliver a column) stays visible rather
                # than silently dropping the claim.
                out.append(base)
                continue
            for col, wfrom, wto in matched:
                # The window's own bounds drive `period_token` (recompute — the
                # base annual token doesn't describe the month window).
                out.append(
                    base.model_copy(
                        update={
                            "delivery_column_name": col,
                            "valid_from": wfrom,
                            "valid_to": wto,
                            "operational_definition": None,
                            "period_token": self._period_token_for_window(wfrom, wto),
                        }
                    )
                )
        out.sort(key=lambda s: (s.valid_from, s.valid_to, s.delivery_column_name or ""))
        return out

    def _variant_slug(self, register_variant_id: int) -> str | None:
        row = self._conn.execute(
            "SELECT slug FROM register_variant WHERE register_variant_id = ?",
            (register_variant_id,),
        ).fetchone()
        return row["slug"] if row else None

    def _resolve_variant_id(
        self, register_id: int, variant_slug: str
    ) -> int | _Missing:
        """register_variant_id for a (register, variant slug) pair, or the
        `_MISSING` sentinel when the slug names no variant under the register (a
        genuine miss → empty `resolve_at` result). `_default` is looked up like
        any other slug: a curated `_default` row matches its id; a variant-less
        register has no `register_variant` row (and no states), so `_default`
        misses → empty (states always carry a real `register_variant_id`; see DESIGN.md → Two-level variable model)."""
        row = self._conn.execute(
            "SELECT register_variant_id FROM register_variant "
            "WHERE register_id = ? AND slug = ?",
            (register_id, variant_slug),
        ).fetchone()
        return row["register_variant_id"] if row is not None else _MISSING

    # ── A2.5 variable-grain edge accessors (see DESIGN.md → Catalog API surface) ──

    @staticmethod
    def _ref_fqid(provider: str, register: str, variable: str) -> Fqid | None:
        """Best-effort 3-segment binding FQID for an edge endpoint (see DESIGN.md → Composite registers and source tracking and FQID grammar).
        The stored triple is exactly the binding FQID now; build-time slug
        validation guarantees it round-trips, but a malformed/NULL slug surfaces
        as None rather than raising (the triple stays the load-bearing identity)."""
        try:
            return Fqid.binding_fqid(provider, register, variable)
        except FqidError:
            return None

    def _edges_for_variable(
        self, provider: str, register: str, variable: str, variable_id: int
    ) -> dict[str, tuple]:
        """All variable-grain edges for one resolved variable, keyed by the
        canonical (provider, register, variable) triple. Factored so `resolve`
        and the standalone accessors share one query set."""
        return {
            "same_as": self._same_as_edges(provider, register, variable),
            "replaced_by": self._successor_edges(provider, register, variable),
            "lineage": self._lineage_edges(variable_id),
        }

    def _same_as_edges(
        self, provider: str, register: str, variable: str
    ) -> tuple[VariableRef, ...]:
        """`variable_same_as` neighbors (a-side keyed; stored both directions)."""
        rows = self._conn.execute(
            "SELECT b_provider, b_register, b_variable FROM variable_same_as "
            "WHERE a_provider = ? AND a_register = ? AND a_variable = ? "
            "ORDER BY b_provider, b_register, b_variable",
            (provider, register, variable),
        ).fetchall()
        return tuple(
            VariableRef(
                fqid=self._ref_fqid(r["b_provider"], r["b_register"], r["b_variable"]),
                provider=r["b_provider"],
                register=r["b_register"],
                variable=r["b_variable"],
            )
            for r in rows
        )

    def _successor_edges(
        self, provider: str, register: str, variable: str
    ) -> tuple[VariableRef, ...]:
        """OUTBOUND succession (`variable_replaced_by`, predecessor side = this
        variable). #142: carries `beskrivning` (reason) + `effective_year`."""
        rows = self._conn.execute(
            "SELECT successor_provider, successor_register, successor_variable, "
            "effective_year, beskrivning FROM variable_replaced_by "
            "WHERE predecessor_provider = ? AND predecessor_register = ? "
            "AND predecessor_variable = ? "
            "ORDER BY successor_provider, successor_register, successor_variable",
            (provider, register, variable),
        ).fetchall()
        return tuple(
            VariableRef(
                fqid=self._ref_fqid(
                    r["successor_provider"],
                    r["successor_register"],
                    r["successor_variable"],
                ),
                provider=r["successor_provider"],
                register=r["successor_register"],
                variable=r["successor_variable"],
                reason=r["beskrivning"],
                effective_year=r["effective_year"],
            )
            for r in rows
        )

    def _predecessor_edges(
        self, provider: str, register: str, variable: str
    ) -> tuple[VariableRef, ...]:
        """INBOUND succession (`variable_replaced_by`, successor side = this
        variable). Uses the A2.5 successor-side index. #142 fields carried."""
        rows = self._conn.execute(
            "SELECT predecessor_provider, predecessor_register, predecessor_variable, "
            "effective_year, beskrivning FROM variable_replaced_by "
            "WHERE successor_provider = ? AND successor_register = ? "
            "AND successor_variable = ? "
            "ORDER BY predecessor_provider, predecessor_register, predecessor_variable",
            (provider, register, variable),
        ).fetchall()
        return tuple(
            VariableRef(
                fqid=self._ref_fqid(
                    r["predecessor_provider"],
                    r["predecessor_register"],
                    r["predecessor_variable"],
                ),
                provider=r["predecessor_provider"],
                register=r["predecessor_register"],
                variable=r["predecessor_variable"],
                reason=r["beskrivning"],
                effective_year=r["effective_year"],
            )
            for r in rows
        )

    @staticmethod
    def _class_ref_fqid(slug: str) -> Fqid | None:
        """Best-effort 2-segment classification FQID for a succession endpoint
        (#571). The stored slug IS the identity; a malformed slug surfaces as None
        rather than raising (mirrors `_ref_fqid` for the variable grain)."""
        try:
            return Fqid.classification_fqid(slug)
        except FqidError:
            return None

    def _classification_successor_edges(
        self, slug: str
    ) -> tuple[ClassificationRef, ...]:
        """OUTBOUND classification succession (`classification_replaced_by`,
        predecessor side = this edition). Keyed on the LITERAL slug — succession
        references the exact edition, so this does NOT same_as-canonicalize."""
        rows = self._conn.execute(
            "SELECT successor_slug, effective_year, note FROM classification_replaced_by "
            "WHERE predecessor_slug = ? ORDER BY successor_slug",
            (slug,),
        ).fetchall()
        return tuple(
            ClassificationRef(
                fqid=self._class_ref_fqid(r["successor_slug"]),
                slug=r["successor_slug"],
                effective_year=r["effective_year"],
                note=r["note"],
            )
            for r in rows
        )

    def _classification_predecessor_edges(
        self, slug: str
    ) -> tuple[ClassificationRef, ...]:
        """INBOUND classification succession (`classification_replaced_by`,
        successor side = this edition). Uses the successor-side index. Keyed on the
        LITERAL slug (no same_as canonicalization — succession is per-edition)."""
        rows = self._conn.execute(
            "SELECT predecessor_slug, effective_year, note FROM classification_replaced_by "
            "WHERE successor_slug = ? ORDER BY predecessor_slug",
            (slug,),
        ).fetchall()
        return tuple(
            ClassificationRef(
                fqid=self._class_ref_fqid(r["predecessor_slug"]),
                slug=r["predecessor_slug"],
                effective_year=r["effective_year"],
                note=r["note"],
            )
            for r in rows
        )

    def _classification_derived_from_edges(
        self, slug: str
    ) -> tuple[ClassificationDerivedFromRef, ...]:
        """OUTBOUND non-temporal derivation refs (#779): classifications this
        specialized classification derives from. Keyed on the literal slug, not
        same_as-canonicalized."""
        rows = self._conn.execute(
            "SELECT e.source_slug AS slug, c.short_name, c.name, e.note "
            "FROM classification_derived_from e "
            "JOIN classification c ON c.slug = e.source_slug "
            "WHERE e.derived_slug = ? "
            "ORDER BY e.source_slug",
            (slug,),
        ).fetchall()
        return tuple(
            ClassificationDerivedFromRef(
                fqid=self._class_ref_fqid(r["slug"]),
                slug=r["slug"],
                short_name=r["short_name"],
                name=r["name"],
                note=r["note"],
            )
            for r in rows
        )

    def _classification_derivative_edges(
        self, slug: str
    ) -> tuple[ClassificationDerivedFromRef, ...]:
        """INBOUND non-temporal derivation refs (#779): classifications derived
        from this source classification. Uses the source-side index."""
        rows = self._conn.execute(
            "SELECT e.derived_slug AS slug, c.short_name, c.name, e.note "
            "FROM classification_derived_from e "
            "JOIN classification c ON c.slug = e.derived_slug "
            "WHERE e.source_slug = ? "
            "ORDER BY e.derived_slug",
            (slug,),
        ).fetchall()
        return tuple(
            ClassificationDerivedFromRef(
                fqid=self._class_ref_fqid(r["slug"]),
                slug=r["slug"],
                short_name=r["short_name"],
                name=r["name"],
                note=r["note"],
            )
            for r in rows
        )

    def _lineage_edges(self, variable_id: int) -> tuple[LineageEdge, ...]:
        """Consumer-side lineage (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage)) for this variable's states (the consumer
        side). A2.6: `source_fqid` is the source state's 3-segment binding FQID,
        joined from the source state's variable → register → provider. Best-effort
        — a NULL/malformed source slug surfaces as None (`_ref_fqid`)."""
        rows = self._conn.execute(
            "SELECT l.consumer_state_id, l.source_state_id, l.valid_from, l.valid_to, "
            "sp.slug AS src_provider, sr.slug AS src_register, sv.slug AS src_variable "
            "FROM variable_state_lineage l "
            "JOIN variable_state cs ON l.consumer_state_id = cs.state_id "
            "JOIN variable_state ss ON l.source_state_id = ss.state_id "
            "JOIN variable sv ON ss.variable_id = sv.variable_id "
            "JOIN register sr ON sv.register_id = sr.register_id "
            "JOIN provider sp ON sr.provider_id = sp.provider_id "
            "WHERE cs.variable_id = ? "
            "ORDER BY l.consumer_state_id, l.source_state_id",
            (variable_id,),
        ).fetchall()
        return tuple(
            LineageEdge(
                consumer_state_id=r["consumer_state_id"],
                source_state_id=r["source_state_id"],
                valid_from=r["valid_from"],
                valid_to=r["valid_to"],
                source_fqid=(
                    self._ref_fqid(
                        r["src_provider"], r["src_register"], r["src_variable"]
                    )
                    if r["src_provider"] and r["src_register"] and r["src_variable"]
                    else None
                ),
            )
            for r in rows
        )

    def _lineage_warning_rows(self, variable_id: int) -> tuple[LineageWarning, ...]:
        """Build-time lineage warnings for this variable's states (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage))."""
        rows = self._conn.execute(
            "SELECT w.consumer_state_id, w.warning_kind, w.message "
            "FROM variable_state_lineage_warning w "
            "JOIN variable_state cs ON w.consumer_state_id = cs.state_id "
            "WHERE cs.variable_id = ? "
            "ORDER BY w.consumer_state_id, w.warning_kind",
            (variable_id,),
        ).fetchall()
        return tuple(
            LineageWarning(
                consumer_state_id=r["consumer_state_id"],
                warning_kind=r["warning_kind"],
                message=r["message"],
            )
            for r in rows
        )

    # ── A2.5 public period-resolution + edge-traversal API (see DESIGN.md → Catalog API surface) ──

    def resolve_at(
        self,
        fqid: str | Fqid,
        period: Period,
        *,
        variant: str | None = None,
        value_set_version: str | None = None,
    ) -> list[VariableState]:
        """Point/range resolution (see DESIGN.md → Catalog API surface): the `VariableState`s whose validity
        intersects `period`, chronological ascending. Length 1 for the common
        single-state-in-one-variant-and-version point query; length N across
        variants (omitting `variant`), range periods crossing transitions, or
        co-delivered classification vintages. Empty list when no state covers the
        period (no exception) — only the binding FQID not resolving raises.

        `period` is polymorphic (see reg_schema/DESIGN.md → Two layers: models vs. validator): int year, period token, range dict
        {"from","to"}, or "_default" (no period filter). `variant` narrows to one
        variant (the Source's `register_variant`); `value_set_version` narrows
        multi-vintage results to a single state by `value_set_version_label`.
        """
        parsed = self._parse_binding(fqid)
        resolved = self._resolve_variable_identity(parsed)
        if resolved is None:
            raise _not_found(parsed)
        var, _ = resolved
        variable_id = var["variable_id"]
        meta = self._lookup_variable_meta(variable_id)

        register_variant_id: int | None = None
        if variant is not None:
            rvid = self._resolve_variant_id(meta["register_id"], variant)
            if isinstance(rvid, _Missing):
                return []  # variant slug names no variant under this register
            register_variant_id = rvid

        bounds = _period_bounds(period)
        states = self._expand_state_windows(
            variable_id,
            self._states_in_bounds(variable_id, register_variant_id, bounds),
            bounds,
        )
        if value_set_version is not None:
            states = [
                s for s in states if s.value_set_version_label == value_set_version
            ]
        return states

    def states(self, fqid: str | Fqid) -> list[VariableState]:
        """see DESIGN.md → Catalog API surface: the variable's full state history (≡ `resolve(fqid).states`)."""
        # Route through _parse_binding (like the edge accessors) so a non-binding
        # FQID fails with the structured `not_a_binding_fqid` error instead of a
        # raw AttributeError off a ResolvedRegister/etc. (reg_webapp wants a 4xx, not 500; see reg_webapp/DESIGN.md → Catalog router structure).
        parsed = self._parse_binding(fqid)
        return list(self._resolve_binding(parsed).states)

    def predecessors(self, fqid: str | Fqid) -> list[VariableRef]:
        """see DESIGN.md → Catalog API surface: variables this binding's variable replaced (inbound succession)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        return list(self._predecessor_edges(provider, register, variable))

    def successors(self, fqid: str | Fqid) -> list[VariableRef]:
        """see DESIGN.md → Catalog API surface: variables that replaced this binding's variable (outbound)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        return list(self._successor_edges(provider, register, variable))

    def variable_chain(self, fqid: str | Fqid) -> list[VariableEdition]:
        """The FULL succession timeline of a variable's chain (#582), oldest first,
        terminal (current) last — the variable-grain dual of `classification_chain`.
        The webapp browse panel renders "this variable has N editions" from it,
        instead of only the immediate `replaced_by` neighbor.

        Ordered by TRAVERSAL, anchored on the QUERIED binding (#588), exactly like
        `classification_chain`: from the canonical triple we step the
        deterministic-first successor edge `[0]` forward to the terminal and the
        deterministic-first predecessor edge `[0]` backward to the root (both off the
        already-ORDER-BY'd `_successor_edges` / `_predecessor_edges`). The chain is
        `reversed(backward)` + canonical + forward, so the ORDER is the walk and the
        `effective_year` is DISPLAY-only (a NULL/undated edge no longer inverts it).
        Anchoring on the queried path means a merge sibling on a DIFFERENT inbound
        branch is never reached — only this binding's own path. The deterministic-
        first pick at a split/merge mirrors `resolve_terminal_successor`.

        Two things the immediate-neighbor accessors (`predecessors`/`successors`)
        don't do:

          - same_as canonicalization: `_resolve_edge_triple` resolves the queried
            binding FQID to the canonical (provider, register, variable) the edge
            tables key on, following `variable_same_as` and variable identity (and
            raising not-found on a dead slug — for a chain we want the canonical LIVE
            binding). The `is_self` edition is this canonical triple.
          - full walk: forward to the terminal (the chain end with no outbound
            successor) and backward to the root, assembling every edition on the
            queried node's path.

        A chain edition may be a DEAD/renamed predecessor with no live `variable` row
        — the #355/#411 renamed-slug model: succession tolerates dead predecessor
        editions by design, and there is NO `variable_replaced_by` validator forbidding
        it (UNLIKE `classification_chain`, where the `classification_replaced_by`
        validator DOES fail the build on a dead endpoint). The walk therefore does NOT
        require each edition to be live: a dead predecessor carries its
        (syntactically-valid) binding `fqid` so a citation 301-redirects to the current
        edition (`_ref_fqid`), with `name` None (no live row to read). `fqid` falls to
        None only on a malformed triple. On the corpus today all 12 edges are live, but
        the model permits a dead predecessor.

        `effective_year`/`reason` per edition come from the OUTBOUND edge by which that
        edition was superseded (the terminal, with no outbound edge, gets None/None).

        A variable with no succession edges returns a single edition (`is_current`
        and `is_self` both True)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        canonical = (provider, register, variable)
        seen = {canonical}

        # Forward: canonical → terminal, stepping the deterministic-first successor
        # edge `[0]`. In step `cur → nxt`, the edge's `effective_year`/`reason` is
        # `cur`'s superseded year/reason, so it's attached to the node we step FROM.
        # The terminal (no outbound edge) keeps (None, None).
        Edge = tuple[str, str, str]
        forward: list[tuple[Edge, int | None, str | None]] = []
        cur = canonical
        canonical_year: int | None = None
        canonical_reason: str | None = None
        succ = self._successor_edges(*cur)
        while succ and (nxt := self._ref_triple(succ[0])) not in seen:
            edge = succ[0]
            if forward:
                forward[-1] = (forward[-1][0], edge.effective_year, edge.reason)
            else:
                canonical_year, canonical_reason = edge.effective_year, edge.reason
            seen.add(nxt)
            forward.append((nxt, None, None))
            cur = nxt
            succ = self._successor_edges(*cur)

        # Backward: canonical → root, stepping the deterministic-first predecessor
        # edge `[0]`. The edge prv→cur is `prv`'s OUTBOUND edge, so its
        # `effective_year`/`reason` is prv's superseded year/reason.
        backward: list[tuple[Edge, int | None, str | None]] = []
        cur = canonical
        pred = self._predecessor_edges(*cur)
        while pred and (prv := self._ref_triple(pred[0])) not in seen:
            edge = pred[0]
            seen.add(prv)
            backward.append((prv, edge.effective_year, edge.reason))
            cur = prv
            pred = self._predecessor_edges(*cur)

        # Chain oldest→current = reversed(backward) + canonical + forward.
        ordered: list[tuple[Edge, int | None, str | None]] = [
            *reversed(backward),
            (canonical, canonical_year, canonical_reason),
            *forward,
        ]
        terminal = ordered[-1][0]
        name_by_triple = self._variable_chain_names(t for t, _, _ in ordered)
        return [
            VariableEdition(
                fqid=self._ref_fqid(*triple),
                provider=triple[0],
                register=triple[1],
                variable=triple[2],
                name=name_by_triple.get(triple),
                effective_year=year,
                reason=reason,
                is_current=(triple == terminal),
                is_self=(triple == canonical),
            )
            for triple, year, reason in ordered
        ]

    @staticmethod
    def _ref_triple(ref: VariableRef) -> tuple[str, str, str]:
        """The load-bearing (provider, register, variable) triple of a
        `VariableRef` edge endpoint — the key the `variable_replaced_by` walk steps
        on (the `fqid` is best-effort, but the triple is always present)."""
        return (ref.provider, ref.register_name, ref.variable)

    def _variable_chain_names(
        self, triples: Iterable[tuple[str, str, str]]
    ) -> dict[tuple[str, str, str], str | None]:
        """Map each chain triple to its live `variable.name`. A DEAD/renamed
        predecessor (no live `variable` row — the #355/#411 model tolerated by
        `variable_chain`; UNLIKE classifications, no validator forbids it) is simply
        absent from the map, so the caller's `name_by_triple.get(triple)` yields None
        for it — exactly the dead-edition behavior. A present-but-NULL name is a live
        row with no name and stays in the map. The variable-grain dual of
        `_classification_chain_rows`, joining provider/register slugs to key on the
        (provider, register, variable) triple."""
        triple_list = list(triples)
        if not triple_list:
            return {}
        placeholders = ",".join("(?, ?, ?)" for _ in triple_list)
        params = [seg for triple in triple_list for seg in triple]
        return {
            (row["provider_slug"], row["register_slug"], row["slug"]): row["name"]
            for row in self._conn.execute(
                "SELECT p.slug AS provider_slug, r.slug AS register_slug, "
                "v.slug, v.name FROM variable v "
                "JOIN register r ON v.register_id = r.register_id "
                "JOIN provider p ON r.provider_id = p.provider_id "
                "WHERE (p.slug, r.slug, v.slug) "
                f"IN ({placeholders})",
                params,
            ).fetchall()
        }

    def classification_successors(self, fqid: str | Fqid) -> list[ClassificationRef]:
        """The editions that replaced this classification edition (outbound
        succession, #571). Keyed on the literal slug — succession tolerates a DEAD
        predecessor edition (a renamed/retired slug still carries edges), so unlike
        `successors` this does NOT require the slug to resolve to a live row."""
        return list(
            self._classification_successor_edges(self._parse_classification(fqid))
        )

    def classification_predecessors(self, fqid: str | Fqid) -> list[ClassificationRef]:
        """The editions this classification edition replaced (inbound succession,
        #571). Keyed on the literal slug; tolerates a dead edition like
        `classification_successors`."""
        return list(
            self._classification_predecessor_edges(self._parse_classification(fqid))
        )

    def classification_derived_from(
        self, fqid: str | Fqid
    ) -> list[ClassificationDerivedFromRef]:
        """The non-temporal source classifications this classification derives
        from (#779). Keyed on the literal slug and intentionally independent of
        classification succession."""
        return list(
            self._classification_derived_from_edges(self._parse_classification(fqid))
        )

    def classification_derivatives(
        self, fqid: str | Fqid
    ) -> list[ClassificationDerivedFromRef]:
        """The non-temporal specialized classifications derived from this
        classification (#779). Keyed on the literal slug and intentionally
        independent of classification succession."""
        return list(
            self._classification_derivative_edges(self._parse_classification(fqid))
        )

    def classification_chain(self, fqid: str | Fqid) -> list[ClassificationEdition]:
        """The FULL edition timeline of a classification's succession chain (#571),
        oldest first, terminal (current) last — what the webapp browse panel
        renders to show "this classification has N editions" instead of only the
        immediate neighbor.

        Ordered by TRAVERSAL, anchored on the QUERIED edition (#588): from the
        canonical slug we walk backward to the root via the deterministic-first
        predecessor (the `[0]` of the already-ORDER-BY'd
        `_classification_predecessor_edges`) and forward via the FULL successor
        CLOSURE. The chain is `reversed(backward)` + canonical + forward-closure, so
        the ORDER is the walk — the `effective_year` is a DISPLAY field, never the
        sort key (a NULL/undated edge no longer inverts the order).

        The forward walk is BRANCH-AWARE (#605): a 1→many succession SPLIT (e.g.
        `sun1996 → {sun2000-niva, sun2000-inriktning, sun2000-grupp}`, #579) fans out
        into every branch, so querying the split ROOT surfaces ALL the downstream
        dimensions rather than only the deterministic-first one. A node with exactly
        ONE successor walks linearly as before; a node with >1 successor recurses into
        ALL of them. The closure is a DFS visiting each node's successors in
        `ORDER BY successor_slug` (deterministic), emitting each branch's editions in
        vintage (traversal) order before descending the next branch. Every started
        node with no ACTIVE outbound successor is `is_current=True` — so a split
        root's chain can have MULTIPLE current editions, one per active branch tip.
        Future-dated successors stay visible in the chain before they are active.

        The BACKWARD walk stays deterministic-first — each classification has ≤1
        predecessor (no merges exist in the corpus), so the `[0]` predecessor IS the
        only one. Anchoring on the queried path means: querying a LEAF (e.g.
        `sun2020-niva`) walks back through its own branch to the split root and then
        forward — but the forward closure FROM a leaf is just the leaf's own
        (childless) subtree, so a leaf's chain is its single linear path
        (`[sun1996, sun2000-niva, sun2020-niva]`), NOT the sibling branches. Only the
        SPLIT NODE itself fans out, and a leaf reaches the split node only on the
        backward (single-predecessor) walk.

        Two things the immediate-neighbor accessors don't do:

          - same_as canonicalization: the queried slug may be a curated
            `classification_same_as` alias (see DESIGN.md → Classifications). We
            resolve it to the canonical LIVE edition's real slug, so the chain (and
            `is_self`) is anchored on the canonical edition, not the alias. A slug
            that resolves to no live row falls back to itself — succession tolerates
            dead slugs.
          - full walk: forward over every successor branch and backward to the root,
            assembling every edition on the queried node's forward closure +
            backward path.

        Every chain endpoint is a live `classification` row — `reg_meta_build`'s
        validator (`validate.py`, the `classification_replaced_by` check) fails the
        build if any succession edge references a slug with no live row — so the
        walk does NOT special-case dead editions; each edition carries its live
        `fqid`/`name`.

        A standalone classification with no succession edges returns a single
        edition (`is_current` and `is_self` both True). The walk lives here rather
        than reusing `queries._classification_editions` because `queries` imports
        `catalog` (catalog is the lower layer — importing back would be circular).

        `effective_year` per edition is the year on the OUTBOUND edge by which that
        edition is superseded by its successor on the path. A future-dated successor
        therefore leaves its predecessor current before the policy year while still
        displaying that upcoming `effective_year`. A SPLIT node's outbound edges may
        disagree; we read the deterministic-first edge `[0]`'s year (in the #579
        split the three edges share the same year, so the pick is moot, but the rule
        is deterministic)."""
        queried = self._parse_classification(fqid)
        canonical = self._canonical_classification_slug(queried)
        seen = {canonical}

        # Forward CLOSURE: canonical → every reachable terminal, fanning out at a
        # split. `_classification_forward_closure` (DFS, ORDER BY successor_slug)
        # emits the closure WITHOUT the canonical node and attaches each node's
        # `effective_year` (its deterministic-first outbound edge's year). The split
        # ROOT's own year is the canonical node's, read off its `[0]` edge below.
        forward = self._classification_forward_closure(canonical, seen)
        canonical_succ = self._classification_successor_edges(canonical)
        canonical_year = canonical_succ[0].effective_year if canonical_succ else None

        # Backward: canonical → root, stepping the deterministic-first predecessor
        # edge `[0]`. The edge prv→cur is `prv`'s OUTBOUND edge, so its
        # `effective_year` is prv's superseded year. Each edition has ≤1 predecessor
        # (no merges), so `[0]` is the only inbound edge.
        backward: list[tuple[str, int | None]] = []
        cur = canonical
        pred = self._classification_predecessor_edges(cur)
        while pred and pred[0].slug not in seen:
            edge = pred[0]
            seen.add(edge.slug)
            backward.append((edge.slug, edge.effective_year))
            cur = edge.slug
            pred = self._classification_predecessor_edges(cur)

        # Chain oldest→current = reversed(backward) + canonical + forward-closure.
        ordered: list[tuple[str, int | None]] = [
            *reversed(backward),
            (canonical, canonical_year),
            *forward,
        ]
        row_by_slug = self._classification_chain_rows(s for s, _ in ordered)
        # Current terminals = started nodes with no ACTIVE outbound successor. Future
        # successors remain visible in the historical chain, but do not make their
        # predecessor non-current until the DB's manifest as-of year reaches the
        # edge year. A split root can still have MULTIPLE current terminals.
        terminals = {
            slug
            for slug, _ in ordered
            if self._classification_edition_started(slug, row_by_slug)
            and not any(
                self._classification_successor_edge_is_active(edge)
                for edge in self._classification_successor_edges(slug)
            )
        }
        editions: list[ClassificationEdition] = []
        for slug, year in ordered:
            # The chain invariant is every slug is a live row (validate.py fails the
            # build otherwise), so the None-guard rarely fires — kept for safety.
            row = row_by_slug.get(slug)
            editions.append(
                ClassificationEdition(
                    slug=slug,
                    fqid=self._class_ref_fqid(slug),
                    name=row["name"] if row else None,
                    effective_year=year,
                    version_year=row["valid_from"] if row else None,
                    is_current=(slug in terminals),
                    is_self=(slug == canonical),
                )
            )
        return editions

    def classification_codes(self, fqid: str | Fqid) -> list[ClassificationCode]:
        """The value-set codes/labels of ONE classification edition (#609), code-
        ordered. Scoped to the RESOLVED edition (`classification_code` keys on
        `classification_id`, and every edition is its own row), so the leaf shows
        the codes of the edition it is viewing — other editions are reached via the
        edition-chain panel (each `class/<slug>` leaf reads its own codes). Resolves
        `same_as` like the other classification accessors, so an alias cites its
        resolved target's codes.

        `is_valid` is canonical / unknown. Observed value-set mismatches are
        state-local conformance warnings, not classification codes. Empty when the
        edition carries no `classification_code` rows. The codes are PUBLIC
        classification codes, not row-level data."""
        resolved = self._resolve_classification(self._coerce_classification_fqid(fqid))
        rows = self._conn.execute(
            "SELECT vc.code, vc.label, cc.level, cc.is_valid "
            "FROM classification_code cc "
            "JOIN value_code vc ON vc.code_id = cc.code_id "
            "WHERE cc.classification_id = ? "
            "ORDER BY vc.code, vc.label",
            (resolved.classification_id,),
        ).fetchall()
        return [
            ClassificationCode(
                code=r["code"],
                label=r["label"],
                level=r["level"],
                # is_valid is 1/0/NULL on the row; keep NULL as None (validity
                # unknown — no canonical CSV exists), else coerce to bool.
                is_valid=None if r["is_valid"] is None else bool(r["is_valid"]),
            )
            for r in rows
        ]

    def classification_dimensions(self, fqid: str | Fqid) -> list[ConceptGroupSummary]:
        """The curated classification umbrella group(s) this edition belongs to
        (#609) — the classification-grain dual of `dimensions` (#516). Surfaces the
        niva ↔ aggregate granularity relationship that #585/#608 model as
        axis-less members of `group:sun` (e.g. `sun2020-niva` alongside the
        7-level `niva-old` and 5-level `niva-grov` aggregates): the leaf reads its
        sibling members from the EXISTING `concept_group_classification` table — no
        new query infra, no browse-fold/group-membership change.

        Resolves `same_as` to the canonical edition (so an alias sees its target's
        groups). Empty for a classification in no umbrella group (the common case —
        only curated SUN-style dimensions are grouped)."""
        # Resolve FIRST so an absent (but syntactically valid) classification
        # FAILS FAST via `_not_found` — fail-fast parity with `classification_codes`.
        # (Don't reuse `_canonical_classification_slug`: it intentionally tolerates
        # dead slugs for the succession chain walk, which would swallow not-found
        # here and make a missing classification look like an ungrouped one.)
        resolved = self._resolve_classification(self._coerce_classification_fqid(fqid))
        _, groups = self._resolved_classification_umbrella(resolved)
        return groups

    def _resolved_classification_umbrella(
        self, resolved: ResolvedClassification
    ) -> tuple[str | None, list[ConceptGroupSummary]]:
        """The resolved edition's CANONICAL live slug + the curated umbrella group(s)
        it belongs to. Group members carry the canonical `class/<slug>`, never a
        same_as alias — so the slug is re-read from the resolved row's id (not the
        possibly-aliased queried FQID), and the umbrella is filtered off
        `list_classification_groups()` by that slug. Slug is None only on a live row
        with a NULL slug (build-prevented) → empty groups. Shared by
        `classification_dimensions` (#609) and `graph_for_classification_fqid`
        (#792)."""
        row = self._conn.execute(
            "SELECT slug FROM classification WHERE id = ?",
            (resolved.classification_id,),
        ).fetchone()
        if row is None or row["slug"] is None:
            return None, []
        slug = row["slug"]
        target = str(Fqid.classification_fqid(slug))
        groups = [
            g
            for g in self.list_classification_groups()
            if any(str(m.fqid) == target for m in g.members)
        ]
        return slug, groups

    @staticmethod
    def _coerce_classification_fqid(fqid: str | Fqid) -> Fqid:
        """Parse/validate a classification FQID (kind-checked via
        `_parse_classification`), returning a `Fqid` for the resolve path."""
        return Fqid.classification_fqid(Catalog._parse_classification(fqid))

    def _classification_forward_closure(
        self, root: str, seen: set[str]
    ) -> list[tuple[str, int | None]]:
        """The forward succession closure from `root` (EXCLUSIVE of `root`), as a
        flat `(slug, effective_year)` list in DFS order (#605). Visits each node's
        successors in `ORDER BY successor_slug` (the `_classification_successor_edges`
        order), emitting a branch's whole subtree before descending the next sibling
        branch — so a split fans out into every branch deterministically.

        `effective_year` per emitted node is the year on its OWN deterministic-first
        outbound edge (the year it was superseded), so a terminal (no outbound edge)
        carries None. `seen` is the shared cycle guard (the caller seeds it with the
        canonical root + any backward-path nodes); a successor already in `seen` is
        skipped, keeping the walk acyclic."""
        out: list[tuple[str, int | None]] = []
        for edge in self._classification_successor_edges(root):
            if edge.slug in seen:
                continue
            seen.add(edge.slug)
            # The node's own outbound year (None at a terminal) — its
            # deterministic-first edge, matching `canonical_year` above.
            child_succ = self._classification_successor_edges(edge.slug)
            child_year = child_succ[0].effective_year if child_succ else None
            out.append((edge.slug, child_year))
            out.extend(self._classification_forward_closure(edge.slug, seen))
        return out

    def _classification_successor_edge_is_active(self, edge: ClassificationRef) -> bool:
        return classification_succession_edge_is_active(
            edge.effective_year, self._classification_as_of_year
        )

    def _classification_edition_started(
        self, slug: str, row_by_slug: dict[str, sqlite3.Row]
    ) -> bool:
        row = row_by_slug.get(slug)
        if row is not None and row["valid_from"] is not None:
            return row["valid_from"] <= self._classification_as_of_year
        inbound_years = [
            edge.effective_year
            for edge in self._classification_predecessor_edges(slug)
            if edge.effective_year is not None
        ]
        return (
            not inbound_years or min(inbound_years) <= self._classification_as_of_year
        )

    def _canonical_classification_slug(self, slug: str) -> str:
        """Resolve a (possibly `same_as`-aliased) classification slug to the
        canonical LIVE edition's real slug. `_resolve_classification` follows the
        same_as graph to a live `classification` row whose id we re-read the slug
        from; an unresolvable slug falls back to itself (succession tolerates dead
        slugs)."""
        try:
            resolved = self._resolve_classification(Fqid.classification_fqid(slug))
        except RegMetaError:
            return slug
        row = self._conn.execute(
            "SELECT slug FROM classification WHERE id = ?",
            (resolved.classification_id,),
        ).fetchone()
        return row["slug"] if row and row["slug"] is not None else slug

    def _classification_chain_rows(
        self, slugs: Iterable[str]
    ) -> dict[str, sqlite3.Row]:
        """Map each chain slug to its live `classification` row (`name` +
        `valid_from`). Every chain slug is a live row (the validator forbids
        succession edges to dead slugs; see `classification_chain`), so the map
        covers every slug; a present-but-NULL `name`/`valid_from` is a live row with
        a NULL column and stays in the map. `valid_from` is the edition's OWN
        point-in-time vintage year (an INTEGER; vintage lives in slug + name +
        valid_from), distinct from the succession-edge `effective_year`."""
        slug_list = list(slugs)
        if not slug_list:
            return {}
        placeholders = ",".join("?" * len(slug_list))
        return {
            row["slug"]: row
            for row in self._conn.execute(
                "SELECT slug, name, valid_from FROM classification "
                f"WHERE slug IN ({placeholders})",
                slug_list,
            ).fetchall()
        }

    def dimensions(self, fqid: str | Fqid) -> list[ConceptGroupSummary]:
        """see DESIGN.md → Catalog API surface: the concept-group dimension
        memberships (the variant facet groups — level / population / rank / …)
        that contain this binding's variable. Resolves `same_as` like the other
        edge accessors, so an alias cites its resolved target's groups (#489)."""
        provider, register, variable, _ = self._resolve_edge_triple(fqid)
        target = str(Fqid.binding_fqid(provider, register, variable))
        return [
            g
            for g in self.list_concept_groups(provider, register)
            if any(str(m.fqid) == target for m in g.members)
        ]

    def graph_for_fqid(self, fqid: str | Fqid) -> RelationshipGraph:
        """The relationship-graph contract for a variable subject (#761): one node
        per variable with its representation-run state history + succession/related
        edges + same_as/group metadata, unioned over the variable's concept-group
        members (Fork B — a member page renders the same group union as the group
        page). Resolves `same_as` and raises `not_a_binding_fqid` / `fqid_not_found`
        like the sibling edge accessors (the webapp's 4xx/301 path). The topology +
        domain predicates live in `graph.py`; this is the thin entry point."""
        from . import graph  # local: graph.py imports catalog (one-directional)

        parsed = self._parse_binding(fqid)
        resolved = self._resolve_binding(parsed)
        return graph.graph_for_fqid(self, resolved)

    def graph_for_classification_fqid(self, fqid: str | Fqid) -> RelationshipGraph:
        """The relationship-graph contract for a classification leaf (#792) — the
        classification analog of `graph_for_fqid`. Resolves the FQID to its canonical
        live edition (raising `not_a_classification_fqid` / `fqid_not_found` like the
        sibling classification accessors, the webapp's 4xx path), then unions the
        edition's curated umbrella group(s) — the niva↔aggregate cross-reference plus
        the edition chain (#678 retires `ClassificationDimensionsPanel` /
        `ClassificationLineagePanels` into this one payload). `focus_id` is the
        canonical edition; the union is empty (don't-render) for a lone edition with
        no chain and no umbrella. The topology lives in `graph.py`; this is the thin
        entry point that owns the resolve (mirroring `graph_for_fqid`)."""
        from . import graph  # local: graph.py imports catalog (one-directional)

        resolved = self._resolve_classification(self._coerce_classification_fqid(fqid))
        # Canonical live slug (for `focus_id` + the ungrouped chain walk) + the
        # edition's umbrella group(s) — shared with `classification_dimensions`.
        canonical_slug, groups = self._resolved_classification_umbrella(resolved)
        if canonical_slug is None:
            raise _not_found(resolved.fqid)  # live row with NULL slug (build-prevented)
        return graph.graph_for_classification_fqid(self, canonical_slug, groups)

    def graph_for_group(
        self, provider_slug: str, register_slug: str, key: str
    ) -> RelationshipGraph | None:
        """The relationship-graph contract for a register concept group (#761),
        keyed by `(provider, register, key)` (NOT an FQID). The union of all member
        variables' graphs, `focus_id=None`. None when the group doesn't exist (the
        webapp maps it to 404). Delegates to `graph.py`."""
        from . import graph  # local: graph.py imports catalog (one-directional)

        return graph.graph_for_concept_group(self, provider_slug, register_slug, key)

    def graph_for_classification_group(self, key: str) -> RelationshipGraph | None:
        """The relationship-graph contract for a classification umbrella group
        (#761), keyed by its derivation `key`. The union of all member editions'
        classification-succession chains, `focus_id=None`. None when the group
        doesn't exist (the webapp maps it to 404). Delegates to `graph.py`."""
        from . import graph  # local: graph.py imports catalog (one-directional)

        return graph.graph_for_classification_group(self, key)

    def lineage(self, fqid: str | Fqid) -> list[LineageEdge]:
        """see DESIGN.md → Catalog API surface: consumer-side composite lineage edges (state grain; see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage))."""
        _, _, _, variable_id = self._resolve_edge_triple(fqid)
        return list(self._lineage_edges(variable_id))

    def lineage_warnings(self, fqid: str | Fqid) -> list[LineageWarning]:
        """see DESIGN.md → Catalog API surface: build-time lineage warnings for the binding (see reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage))."""
        _, _, _, variable_id = self._resolve_edge_triple(fqid)
        return list(self._lineage_warning_rows(variable_id))

    def resolve_terminal_successor(self, fqid: str | Fqid) -> Fqid | None:
        """Walk the succession chain from a (possibly DEAD/renamed) FQID to its
        TERMINAL successor — the chain end with NO further outbound edge. Returns
        that terminal as an `Fqid` of the same kind, or None when the start has no
        outbound succession at all (genuinely unknown — the caller should 404).
        Used by the webapp to 301-redirect a citation of a renamed slug to where it
        lives now (#355 PART 2; register grain added in #412, classification in
        #571).

        Dispatches on FQID kind:
          - VARIABLE_BINDING → walks `variable_replaced_by` on the stored
            (provider, register, variable) triple, returns a binding `Fqid`.
          - REGISTER → walks `register_replaced_by` on the stored
            (provider, register) pair, returns a register `Fqid`.
          - CLASSIFICATION → walks `classification_replaced_by` on the stored
            edition slug, returns a classification `Fqid` (e.g. an old vintage
            edition redirects to the current one).
          - PROVIDER → None: there is no succession table for providers, so a
            rename there has nowhere to redirect.

        Unlike `successors` / `_resolve_edge_triple`, this does NOT require the
        FQID to resolve to a live row — that is the whole point: a renamed slug
        404s (its `variable` / `register` / `classification` row is gone), and we
        walk purely on the stored string tuple in the succession table to find
        where the citation should redirect.

        Always walks to the ABSOLUTE chain end, never hop-by-hop: a webapp 301 can
        be cached, and double-rename churn (A→B then B→C) would leave a cached
        A→B redirect pointing at a now-dead intermediate. Resolving to the terminal
        every time keeps the redirect correct under churn.

        Split simplification: the schema does NOT enforce 1:1 succession — a
        predecessor may have several successors (a split). Renames are 1:1, so this
        is rare; when it happens we pick deterministically (ORDER BY successor
        tuple, first row) so a 301 stays stable and never 404s, rather than
        refusing. Cycle guard: a `seen` set defends against a malformed
        double-rename loop (A→B→A) in the DB so the walk always terminates.
        """
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        if parsed.kind is FqidKind.VARIABLE_BINDING:
            assert parsed.provider and parsed.register and parsed.variable
            start = (parsed.provider, parsed.register, parsed.variable)
            terminal = self._walk_terminal(start, self._first_successor_triple)
            return None if terminal is None else Fqid.binding_fqid(*terminal)
        if parsed.kind is FqidKind.REGISTER:
            assert parsed.provider and parsed.register
            pair = (parsed.provider, parsed.register)
            terminal = self._walk_terminal(pair, self._first_register_successor_pair)
            return None if terminal is None else Fqid.register_fqid(*terminal)
        if parsed.kind is FqidKind.CLASSIFICATION:
            assert parsed.classification
            # 1-tuple start: `_first_classification_successor_slug(*current)`
            # unpacks `(slug,)` to the single slug arg.
            cstart = (parsed.classification,)
            terminal = self._walk_terminal(
                cstart, self._first_classification_successor_slug
            )
            return None if terminal is None else Fqid.classification_fqid(*terminal)
        # PROVIDER grain has no succession table — a rename there has nowhere to
        # redirect. Bail before any SQL.
        return None

    @staticmethod
    def _walk_terminal(
        start: _SuccTuple, first_successor: Callable[..., _SuccTuple | None]
    ) -> _SuccTuple | None:
        """Shared chain walk for `resolve_terminal_successor`. Follows
        `first_successor` from `start` to the ABSOLUTE chain end and returns the
        terminal tuple, or None when `start` has no outbound edge at all. The
        per-grain difference is only the `first_successor` accessor (and so the
        tuple arity); the walk semantics — terminal = no outbound edge, `seen`
        cycle guard, None when the start is itself terminal — are identical."""
        seen: set[_SuccTuple] = {start}
        current = start
        while True:
            nxt = first_successor(*current)
            if nxt is None or nxt in seen:
                # Terminal (no outbound edge) or a defensive cycle break.
                break
            seen.add(nxt)
            current = nxt
        if current == start:
            return None  # start had no outbound edge — genuinely unknown
        return current

    def _first_successor_triple(
        self, provider: str, register: str, variable: str
    ) -> tuple[str, str, str] | None:
        """The deterministically-first outbound `variable_replaced_by` successor
        triple for a predicate triple, or None when there is none. Reuses the
        `_successor_edges` SELECT shape (predecessor-side keyed, ORDER BY successor
        triple) but reads only the successor triple — the walk needs no `#142`
        reason/effective_year. ORDER BY + LIMIT 1 makes the split pick (see
        `resolve_terminal_successor`) deterministic in SQL."""
        row = self._conn.execute(
            "SELECT successor_provider, successor_register, successor_variable "
            "FROM variable_replaced_by "
            "WHERE predecessor_provider = ? AND predecessor_register = ? "
            "AND predecessor_variable = ? "
            "ORDER BY successor_provider, successor_register, successor_variable "
            "LIMIT 1",
            (provider, register, variable),
        ).fetchone()
        if row is None:
            return None
        return (
            row["successor_provider"],
            row["successor_register"],
            row["successor_variable"],
        )

    def _first_register_successor_pair(
        self, provider: str, register: str
    ) -> tuple[str, str] | None:
        """The deterministically-first outbound `register_replaced_by` successor
        pair for a predecessor (provider, register), or None when there is none.
        Register-grain analogue of `_first_successor_triple`: predecessor-side
        keyed, ORDER BY + LIMIT 1 makes the split pick (see
        `resolve_terminal_successor`) deterministic in SQL. Reads only the
        successor pair — the walk needs no effective_year/note."""
        row = self._conn.execute(
            "SELECT successor_provider, successor_register "
            "FROM register_replaced_by "
            "WHERE predecessor_provider = ? AND predecessor_register = ? "
            "ORDER BY successor_provider, successor_register "
            "LIMIT 1",
            (provider, register),
        ).fetchone()
        if row is None:
            return None
        return (row["successor_provider"], row["successor_register"])

    def _first_classification_successor_slug(self, slug: str) -> tuple[str] | None:
        """The deterministically-first outbound `classification_replaced_by`
        successor slug for a predecessor edition, or None when there is none.
        Classification-grain analogue of `_first_successor_triple` (#571):
        predecessor-keyed, ORDER BY + LIMIT 1 makes the split pick (see
        `resolve_terminal_successor`) deterministic in SQL. Returns a 1-tuple so
        `_walk_terminal` preserves arity across grains; reads only the successor
        slug — the walk needs no effective_year/note."""
        row = self._conn.execute(
            "SELECT successor_slug FROM classification_replaced_by "
            "WHERE predecessor_slug = ? "
            "AND (effective_year IS NULL OR effective_year <= ?) "
            "ORDER BY successor_slug LIMIT 1",
            (slug, self._classification_as_of_year),
        ).fetchone()
        if row is None:
            return None
        return (row["successor_slug"],)

    def _resolve_edge_triple(self, fqid: str | Fqid) -> tuple[str, str, str, int]:
        """Resolve a binding FQID to the canonical (provider, register, variable,
        variable_id) the edge tables key on. Raises `_not_found` when the binding
        doesn't resolve (consistent with `resolve`/`states`)."""
        parsed = self._parse_binding(fqid)
        resolved = self._resolve_variable_identity(parsed)
        if resolved is None:
            raise _not_found(parsed)
        var, _ = resolved
        meta = self._lookup_variable_meta(var["variable_id"])
        return (
            meta["provider_slug"],
            meta["register_slug"],
            meta["slug"],
            var["variable_id"],
        )

    @staticmethod
    def _parse_binding(fqid: str | Fqid) -> Fqid:
        """Parse a binding FQID and assert it is one. The catalog API accessors (see DESIGN.md → Catalog API surface) only
        accept binding FQIDs; a non-binding kind is a usage error."""
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        if parsed.kind is not FqidKind.VARIABLE_BINDING:
            raise RegMetaError(
                exit_code=EXIT_USAGE,
                code="not_a_binding_fqid",
                error_class="query",
                message=(
                    f"expected a variable-binding FQID, got "
                    f"kind={parsed.kind.value}: {parsed!s}"
                ),
                remediation="Pass a 3-segment binding FQID (provider/register/slug).",
            )
        return parsed

    @staticmethod
    def _parse_classification(fqid: str | Fqid) -> str:
        """Parse a classification FQID and return its literal slug. The
        classification succession accessors only accept classification FQIDs; a
        non-classification kind is a usage error (mirrors `_parse_binding`)."""
        parsed = parse(fqid) if isinstance(fqid, str) else parse(str(fqid))
        if parsed.kind is not FqidKind.CLASSIFICATION:
            raise RegMetaError(
                exit_code=EXIT_USAGE,
                code="not_a_classification_fqid",
                error_class="query",
                message=(
                    f"expected a classification FQID, got "
                    f"kind={parsed.kind.value}: {parsed!s}"
                ),
                remediation="Pass a 2-segment classification FQID (class/<slug>).",
            )
        assert parsed.classification
        return parsed.classification

    def _resolve_classification(self, fqid: Fqid) -> ResolvedClassification:
        direct = self._resolve_classification_direct(fqid)
        if direct is not None:
            return direct
        via = self._resolve_classification_via_same_as(fqid)
        if via is not None:
            return via
        raise _not_found(fqid)

    def _resolve_classification_direct(
        self, fqid: Fqid
    ) -> ResolvedClassification | None:
        # A2.6.1: the slug bakes in the vintage and is globally UNIQUE, so a
        # single-bind slug lookup hits at most one row. The old (slug, version)
        # two-bind and the publisher-disambiguation branch are gone — there is
        # no cross-publisher (slug, version) collision to narrow.
        assert fqid.classification
        row = self._conn.execute(
            "SELECT id, short_name, name FROM classification WHERE slug = ?",
            (fqid.classification,),
        ).fetchone()
        if not row:
            return None
        # Outbound succession edges (#571) key on the resolved row's OWN slug
        # (`fqid.classification` == the row's slug on a direct hit). The same_as
        # path `model_copy`s off this direct hit (update={fqid, via_same_as}), so
        # it inherits the resolved edition's edges (only those two are overridden).
        return ResolvedClassification(
            fqid=fqid,
            classification_id=row["id"],
            short_name=row["short_name"],
            name=row["name"],
            replaced_by=self._classification_successor_edges(fqid.classification),
            derived_from=self._classification_derived_from_edges(fqid.classification),
            derivatives=self._classification_derivative_edges(fqid.classification),
        )

    def _class_same_as_source_keys(self) -> frozenset[tuple[str, str]]:
        if self._class_same_as_sources is None:
            rows = self._conn.execute(
                "SELECT DISTINCT a_provider, a_classification_slug "
                "FROM classification_same_as"
            ).fetchall()
            self._class_same_as_sources = frozenset((r[0], r[1]) for r in rows)
        return self._class_same_as_sources

    def _resolve_classification_via_same_as(
        self, fqid: Fqid
    ) -> ResolvedClassification | None:
        """BFS through `classification_same_as`.

        A2.6.1: the slug bakes in the vintage and is globally UNIQUE, so each
        slug maps to exactly one row and the FQID needs no version. This path is
        only reached when the direct (single-bind) lookup missed — i.e. the
        queried slug has no row — which is exactly the same_as use case: a
        caller's old/equivalent slug that's been retired in favor of a present
        one. The BFS seeds from the edge sources naming the queried slug
        (edges carry a provider; the FQID grammar has none), walks edges keyed
        (provider, classification_slug), and resolves each neighbor with a
        single-bind slug lookup.
        """
        assert fqid.classification
        # Seed the provider from the edge table, NOT a classification-row lookup:
        # we're here precisely because the queried slug has no row, so the seed
        # provider can only come from the edges that name this slug as a source.
        sources = self._class_same_as_source_keys()
        seeds = [p for (p, slug) in sources if slug == fqid.classification]
        if not seeds:
            return None
        visited: set[tuple[str, str]] = {(p, fqid.classification) for p in seeds}
        queue: deque[tuple[str, str, tuple[Fqid, ...]]] = deque()
        for p in seeds:
            queue.append((p, fqid.classification, ()))
        while queue:
            prov, slug, path = queue.popleft()
            rows = self._conn.execute(
                "SELECT b_provider, b_classification_slug FROM classification_same_as "
                "WHERE a_provider = ? AND a_classification_slug = ?",
                (prov, slug),
            ).fetchall()
            for row in rows:
                n_prov = row["b_provider"]
                n_slug = row["b_classification_slug"]
                key = (n_prov, n_slug)
                if key in visited:
                    continue
                visited.add(key)
                try:
                    n_fqid = Fqid.classification_fqid(n_slug)
                except FqidError:
                    continue
                # Slug is globally UNIQUE: at most one row, no version loop,
                # no publisher disambiguation. A direct miss means the slug
                # isn't in the DB (deleted post-build) — keep walking the edge
                # graph in case a further hop lands on a present row.
                hit = self._resolve_classification_direct(n_fqid)
                if hit is not None:
                    return hit.model_copy(
                        update={"fqid": fqid, "via_same_as": (*path, n_fqid)}
                    )
                queue.append((n_prov, n_slug, (*path, n_fqid)))
        return None


_DISPATCH = {
    FqidKind.PROVIDER: Catalog._resolve_provider,
    FqidKind.REGISTER: Catalog._resolve_register,
    FqidKind.VARIABLE_BINDING: Catalog._resolve_binding,
    FqidKind.CLASSIFICATION: Catalog._resolve_classification,
}
