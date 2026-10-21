"""Materialize explicitly resolved catalog variables and code memberships.

Identity, canonical text, flags, and explicit state windows are curation inputs.
This writer only assigns storage IDs and builds the normal catalog database.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from graphlib import CycleError, TopologicalSorter
from itertools import combinations
from typing import TYPE_CHECKING, Annotated, Literal, Self

from pydantic import (
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from reg_meta_build._curation import (
    SentinelCode,
    curation_error,
)
from reg_meta_build._resolved_common import (
    _VALID_TO_SENTINEL,
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR,
    _provider_id_for,
    _require_trimmed,
    _ResolvedDeliveryScope,
    _ResolvedModel,
    _ResolvedWindow,
)
from reg_meta_build.slug_grammar import validate_slug

from .source_evidence import canonical_sha256

if TYPE_CHECKING:
    from collections.abc import Collection

CURATION_TREE_SHA256_KEY = "curation_tree_sha256"


class ResolvedRegister(_ResolvedModel):
    provider: str
    slug: str
    name: str
    purpose: str | None = None

    _name = field_validator("name")(_require_trimmed)

    @model_validator(mode="after")
    def _identity(self) -> Self:
        validate_slug(self.provider, "provider")
        validate_slug(self.slug, "register")
        return self


class ResolvedVariant(_ResolvedModel):
    slug: str
    name: str
    description: str | None = None
    display_group: str | None = None
    panel_entity_key: str | tuple[str, ...] | None = None
    panel_time_key: str | tuple[str, ...] | None = None
    panel_time_grain: Literal["delivery", "row"] | None = None

    _name = field_validator("name")(_require_trimmed)

    @field_validator("slug")
    @classmethod
    def _slug(cls, value: str) -> str:
        validate_slug(value, "register_variant", allow_default=True)
        return value


class ResolvedCodeSet(_ResolvedModel):
    """Exact membership already resolved for the containing state's period.

    Codes are strings, including empty tokens and leading zeros. Distinct labels
    for one code remain distinct; neither text nor meaning is normalized here.
    Unknown or deliberately withheld membership is a state's ``None`` value.
    """

    members: tuple[tuple[str, str], ...] = Field(min_length=1)

    @field_validator("members")
    @classmethod
    def _canonical_members(
        cls, value: tuple[tuple[str, str], ...]
    ) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(set(value)))


class ResolvedPopulation(_ResolvedModel):
    name: str
    definition: str | None = None
    comment: str | None = None
    date_range: str | None = None

    _name = field_validator("name")(_require_trimmed)


class ResolvedObjectType(_ResolvedModel):
    name: str
    definition: str | None = None

    _name = field_validator("name")(_require_trimmed)


class ResolvedEdition(_ResolvedModel):
    """Edition prose is independent of the catalog's effective state periods."""

    register_ref: ResolvedRegister = Field(alias="register")
    variant: ResolvedVariant
    name: str
    description: str | None = None
    measurement_information: str | None = None
    documentation_status: str | None = None
    first_approved_at: str | None = None
    last_approved_at: str | None = None
    populations: tuple[ResolvedPopulation, ...] = ()
    object_types: tuple[ResolvedObjectType, ...] = ()

    _name = field_validator("name")(_require_trimmed)

    @model_validator(mode="after")
    def _unique_children(self) -> Self:
        for children in (self.populations, self.object_types):
            names = [child.name for child in children]
            if len(names) != len(set(names)):
                raise ValueError("duplicate named metadata within one edition")
        return self


class ResolvedClassificationCode(_ResolvedModel):
    code: str
    label: str
    level: int | None = None
    # The parent code in the same book, as the codebook's `parent_code` cell
    # delivers it; None for a top-level code or a book without a source tree.
    parent_code: str | None = None


class ResolvedClassification(_ResolvedModel):
    """A selected canonical codebook; binding and precedence are already resolved."""

    slug: str
    short_name: str
    name: str
    name_en: str | None = None
    publisher: str | None = None
    valid_from: int | None = None
    valid_to: int | None = None
    description: str | None = None
    url: str | None = None
    codes: tuple[ResolvedClassificationCode, ...] = Field(min_length=1)
    # Curated per-classification sentinel codes (`{code, meaning}` exact
    # strings from `curation/classifications/`). Observed codes on this
    # list keep the binding with a warning instead of severing it. Conformance
    # curation, not codebook content: not part of the pinned content hash.
    sentinel_codes: tuple[SentinelCode, ...] = ()

    _names = field_validator("name", "short_name")(_require_trimmed)

    @field_validator("slug")
    @classmethod
    def _slug(cls, value: str) -> str:
        validate_slug(value, "classification")
        return value

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if (
            self.valid_from is not None
            and self.valid_to is not None
            and self.valid_from > self.valid_to
        ):
            raise ValueError("classification validity bounds are reversed")
        pairs = [(code.code, code.label) for code in self.codes]
        if len(pairs) != len(set(pairs)):
            raise ValueError("duplicate canonical classification code/label pair")
        sentinels = [sentinel.code for sentinel in self.sentinel_codes]
        if len(sentinels) != len(set(sentinels)):
            raise ValueError("duplicate classification sentinel code")
        overlap = sorted(set(sentinels) & {code.code for code in self.codes})
        if overlap:
            raise ValueError(
                f"classification {self.slug!r} lists curated sentinel codes "
                f"also in the canonical code set: {overlap!r}."
            )
        _check_code_tree(self.slug, self.codes)
        return self


def _check_code_tree(slug: str, codes: tuple[ResolvedClassificationCode, ...]) -> None:
    """Require each parent to be one code of the same book, acyclic and shallower."""
    by_code: dict[str, list[ResolvedClassificationCode]] = defaultdict(list)
    for code in codes:
        by_code[code.code].append(code)
    graph: dict[str, tuple[str, ...]] = {}
    for code in codes:
        if code.parent_code is None:
            continue
        parents = by_code.get(code.parent_code, [])
        if len(parents) != 1:
            raise ValueError(
                f"classification {slug!r} code {code.code!r} names parent "
                f"{code.parent_code!r}, which is not exactly one code of the book"
            )
        parent = parents[0]
        if (
            code.level is not None
            and parent.level is not None
            and parent.level >= code.level
        ):
            raise ValueError(
                f"classification {slug!r} code {code.code!r} (level {code.level}) "
                f"names parent {parent.code!r} at level {parent.level}"
            )
        graph[code.code] = (code.parent_code,)
    try:
        TopologicalSorter(graph).prepare()
    except CycleError as exc:
        raise ValueError(
            f"classification {slug!r} parent codes form a cycle: {exc.args[1]!r}"
        ) from None


class ResolvedScopedSentinels(_ResolvedWindow):
    """Build-time certificate from a checked, finite source sentinel decision."""

    delivery_column_name: str
    classification_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_fingerprints: tuple[
        Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")], ...
    ] = Field(min_length=1)
    members: tuple[tuple[str, str], ...] = Field(min_length=1)
    # The decision's `curated:<kind>` head and source locators (#1708).
    provenance: str

    _text = field_validator("delivery_column_name", "provenance")(_require_trimmed)

    @model_validator(mode="after")
    def _unique_evidence(self) -> Self:
        if len(set(self.source_fingerprints)) != len(self.source_fingerprints):
            raise ValueError("duplicate scoped sentinel source fingerprint")
        if any(not code or not label for code, label in self.members) or len(
            {code for code, _ in self.members}
        ) != len(self.members):
            raise ValueError(
                "scoped sentinels need unique literal nonempty code-label pairs"
            )
        return self


class ResolvedConformance(_ResolvedModel):
    """The common resolver's explicit conformance decision, including omissions."""

    declared_classification: str
    status: Literal["conforming", "extended"]
    checked_codes: tuple[str, ...]
    nonconforming_members: tuple[tuple[str, str], ...] = ()
    # Observed non-canonical members whose code is on the declared
    # classification's global sentinel list or an explicitly certified finite
    # source decision. Kept as variable-local codes, never classification_code.
    # The build-time certificate is not a global codebook mutation.
    sentinel_members: tuple[tuple[str, str], ...] = ()
    scoped_sentinels: tuple[ResolvedScopedSentinels, ...] = ()

    @field_validator("declared_classification")
    @classmethod
    def _slug(cls, value: str) -> str:
        validate_slug(value, "classification")
        return value

    @model_validator(mode="after")
    def _unique_members(self) -> Self:
        if len(self.checked_codes) != len(set(self.checked_codes)):
            raise ValueError("duplicate conformance checked code")
        if len(self.nonconforming_members) != len(set(self.nonconforming_members)):
            raise ValueError("duplicate nonconforming member")
        if len(self.sentinel_members) != len(set(self.sentinel_members)):
            raise ValueError("duplicate sentinel member")
        if any(
            not set(certificate.members) <= set(self.sentinel_members)
            for certificate in self.scoped_sentinels
        ):
            raise ValueError(
                "scoped sentinel certificate members must be recorded sentinels"
            )
        if {code for code, _ in self.sentinel_members} & {
            code for code, _ in self.nonconforming_members
        }:
            raise ValueError("sentinel member overlaps nonconforming member")
        expected_status = (
            "extended"
            if self.nonconforming_members or self.sentinel_members
            else "conforming"
        )
        if self.status != expected_status:
            raise ValueError("conformance status disagrees with source extensions")
        return self


class ResolvedClassificationSuccession(_ResolvedModel):
    predecessor: str
    successor: str
    effective_year: int | None = None
    note: str | None = None

    @field_validator("predecessor", "successor")
    @classmethod
    def _slug(cls, value: str) -> str:
        validate_slug(value, "classification")
        return value


class ResolvedClassificationLink(_ResolvedModel):
    """One source-declared book and its independently checked state domain."""

    classification: str
    conformance: ResolvedConformance | None = None
    provenance: str | None = None

    @field_validator("classification")
    @classmethod
    def _slug(cls, value: str) -> str:
        validate_slug(value, "classification")
        return value

    @model_validator(mode="after")
    def _book(self) -> Self:
        if (
            self.conformance is not None
            and self.conformance.declared_classification != self.classification
        ):
            raise ValueError("classification link and conformance decision disagree")
        return self


def _validate_classification_domain(
    value_set: ResolvedCodeSet | None,
    links: tuple[ResolvedClassificationLink, ...],
) -> None:
    slugs = [link.classification for link in links]
    if slugs != sorted(set(slugs)):
        raise ValueError("classification links must have unique sorted books")
    for link in links:
        if link.conformance is None:
            continue
        if value_set is None:
            raise ValueError("conformance requires an effective value set")
        conformance = link.conformance
        members = set(value_set.members)
        checked = set(conformance.checked_codes)
        if checked != {code for code, _ in members}:
            raise ValueError(
                "conformance must check every distinct code in the value set"
            )
        nonconforming = set(conformance.nonconforming_members)
        if (
            not nonconforming <= members
            or not {code for code, _ in nonconforming} <= checked
        ):
            raise ValueError("nonconforming members must be checked domain members")


class ResolvedState(_ResolvedDeliveryScope):
    variant: ResolvedVariant
    delivery_column_name: str
    data_type: str | None
    data_length: str | None
    operational_definition: str | None
    provenance: str | None
    source_register_text: str | None = None
    definition: str | None = None
    measurement_unit: str | None = None
    name: str | None = None
    description: str | None = None
    # Y-202: True when the state spans a pooled multi-year edition range with no
    # explicit annual coverage — one marked state over the whole pooled range,
    # never inferred annual availability. False for every other state.
    pooled: bool = False
    value_set: ResolvedCodeSet | None = None
    value_set_version_label: str = ""
    classification_links: tuple[ResolvedClassificationLink, ...] = ()

    _column = field_validator("delivery_column_name")(_require_trimmed)

    @model_validator(mode="after")
    def _independent_pooling(self) -> Self:
        if self.period_scope == "year_independent" and self.pooled:
            raise ValueError("year-independent delivery cannot carry a pooled flag")
        return self

    @model_validator(mode="after")
    def _classification_contract(self) -> Self:
        _validate_classification_domain(self.value_set, self.classification_links)
        return self


class ResolvedAliasWindow(_ResolvedWindow):
    provenance: str | None = None
    column_metadata: Literal["shared", "per_column"] = "shared"
    data_type: str | None = None
    data_length: str | None = None
    operational_definition: str | None = None
    source_register_text: str | None = None
    definition: str | None = None
    measurement_unit: str | None = None
    name: str | None = None
    description: str | None = None
    coding_metadata: Literal["shared", "per_column"] = "shared"
    value_set: ResolvedCodeSet | None = None
    value_set_version_label: str = ""
    classification_links: tuple[ResolvedClassificationLink, ...] = ()

    @model_validator(mode="after")
    def _column_scope(self) -> Self:
        if self.column_metadata == "shared" and (
            any(
                value is not None
                for value in (
                    self.data_type,
                    self.data_length,
                    self.operational_definition,
                    self.source_register_text,
                    self.definition,
                    self.measurement_unit,
                    self.name,
                    self.description,
                )
            )
        ):
            raise ValueError(
                "shared representation column metadata comes from its state"
            )
        if self.coding_metadata == "shared" and (
            self.value_set is not None
            or self.value_set_version_label
            or self.classification_links
        ):
            raise ValueError("shared representation coding comes from its state")
        if self.coding_metadata == "per_column" and self.value_set is None:
            raise ValueError("per-column coding requires a complete finite domain")
        _validate_classification_domain(self.value_set, self.classification_links)
        return self


class ResolvedAlias(_ResolvedModel):
    variant: ResolvedVariant
    delivery_column_name: str
    windows: tuple[ResolvedAliasWindow, ...] = ()

    _column = field_validator("delivery_column_name")(_require_trimmed)

    @model_validator(mode="after")
    def _disjoint(self) -> Self:
        previous = None
        for window in sorted(self.windows, key=lambda w: w.valid_from):
            if previous is not None and window.valid_from <= previous.valid_to:
                raise ValueError("overlapping windows for one resolved alias")
            previous = window
        return self


# Every state fact except its window: two day-adjacent states that agree on all of
# them are one delivery state.
_STATE_WINDOW = frozenset({"valid_from", "valid_to"})


def _certificate_key(
    certificate: ResolvedScopedSentinels,
) -> tuple[str, str, tuple[tuple[str, str], ...]]:
    return (
        certificate.delivery_column_name,
        certificate.classification_sha256,
        certificate.members,
    )


def _link_facts(
    link: ResolvedClassificationLink, valid_from: str | None, valid_to: str | None
) -> object:
    """A link with its sentinel certificates reduced to what they certify.

    A certificate's window, fingerprints and provenance are its decision's
    evidence, not a state fact: two adjacent decisions certifying the same
    members over the same book and column leave one state (#1442). Only
    certificates that each cover the whole state window reduce. Otherwise the
    link's facts carry the state's own window, so the state never merges (two
    states carrying the same partial certificates would, and the merged
    certificate's hull would then certify the gap between them); the catalog
    validator refuses the uncovered certificate per state.
    """
    conformance = link.conformance
    if conformance is None or not conformance.scoped_sentinels:
        return link
    if (
        valid_from is None
        or valid_to is None
        or not all(
            c.valid_from <= valid_from and valid_to <= c.valid_to
            for c in conformance.scoped_sentinels
        )
    ):
        return link, valid_from, valid_to
    return (
        link.model_copy(
            update={
                "conformance": conformance.model_copy(update={"scoped_sentinels": ()})
            }
        ),
        frozenset(map(_certificate_key, conformance.scoped_sentinels)),
    )


def _state_facts(state: ResolvedState) -> tuple[object, ...]:
    return tuple(
        tuple(
            _link_facts(link, state.valid_from, state.valid_to)
            for link in state.classification_links
        )
        if name == "classification_links"
        else getattr(state, name)
        for name in type(state).model_fields
        if name not in _STATE_WINDOW
    )


def _merged_links(run: list[ResolvedState]) -> tuple[ResolvedClassificationLink, ...]:
    """A merged run's links, with one certificate per certified key.

    A run merges only when each certificate of a state covers that state
    (`_link_facts`), so the union of a key's certificates over a run of
    day-adjacent states covers its hull without a gap.
    """
    links = []
    for index, link in enumerate(run[0].classification_links):
        if link.conformance is None:
            links.append(link)
            continue
        # One decision's certificate repeats on every state it covers.
        unique: dict[ResolvedScopedSentinels, None] = {}
        for state in run:
            conformance = state.classification_links[index].conformance
            assert conformance is not None
            unique.update(dict.fromkeys(conformance.scoped_sentinels))
        groups: dict[object, list[ResolvedScopedSentinels]] = defaultdict(list)
        for certificate in unique:
            groups[_certificate_key(certificate)].append(certificate)
        certificates = []
        for group in groups.values():
            group.sort(key=lambda c: (c.valid_from, c.valid_to))
            certificates.append(
                group[0].model_copy(
                    update={
                        "valid_to": max(c.valid_to for c in group),
                        "source_fingerprints": tuple(
                            dict.fromkeys(
                                f for c in group for f in c.source_fingerprints
                            )
                        ),
                        "provenance": "\n".join(
                            dict.fromkeys(c.provenance for c in group)
                        ),
                    }
                )
            )
        links.append(
            link.model_copy(
                update={
                    "conformance": link.conformance.model_copy(
                        update={"scoped_sentinels": tuple(certificates)}
                    )
                }
            )
        )
    return tuple(links)


def merge_adjacent_states(
    states: tuple[ResolvedState, ...],
) -> tuple[ResolvedState, ...]:
    """Merge day-adjacent dated states whose resolved facts are identical (#1296 2d).

    Occurrence resolution and coding cut a column wherever the set of active
    source editions or code lists changes, so consecutive editions that state
    the same facts arrive as separate states. A run of such states on one
    variant and delivery column, each starting the day after the previous one
    ends, is one state over the run's hull: same value set and version label,
    classification links, data type and length, text fields, provenance and
    pooled flag. When every sentinel certificate on a link covers its own
    state's whole window, a certificate is compared by the members, book and
    column it certifies, and the merged state keeps one certificate per such key
    over the union of its windows, fingerprints and provenance: its decision's
    window and case id are evidence the certificate carries, not a state fact.
    A link with a certificate that does not cover its state is compared whole,
    so the states stay apart and the catalog validator refuses that certificate.
    Population is a variant fact (LISA carries one per variant,
    every other reader none), so one variant never mixes two. A gap, or any
    differing fact, keeps states apart; windows and coverage are unchanged.
    An open-ended state never merges with a closed run: readers count an
    open-ended state as its opening year only, so absorbing the closed years
    into it would drop them from coverage.
    The merged state keeps its earliest segment's `valid_from` and so its
    `state_id`; the absorbed segments' IDs stop resolving. Year-independent
    states have no neighbours and pass through.
    """
    merged: list[ResolvedState | None] = list(states)
    # One lane per value-set version: states of one variant and version never
    # overlap, so a lane sorted by start is a total order and the result cannot
    # depend on input order. States of another version may overlap the lane and
    # never interrupt it.
    lanes: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for index, state in enumerate(states):
        if state.period_scope == "intervals":
            lanes[
                state.variant.slug,
                state.delivery_column_name,
                state.value_set_version_label,
            ].append(index)
    runs: dict[int, list[ResolvedState]] = {}
    for indices in lanes.values():
        head: int | None = None
        for index in sorted(indices, key=lambda i: states[i].valid_from or ""):
            state = states[index]
            current = merged[head] if head is not None else None
            if (
                head is not None
                and current is not None
                and current.valid_to is not None
                and state.valid_from is not None
                and state.valid_to != _VALID_TO_SENTINEL
                and date.fromisoformat(current.valid_to).toordinal() + 1
                == date.fromisoformat(state.valid_from).toordinal()
                # The previous member, not `current`: the hull so far outgrows
                # the head's certificates, which cover only the head's window.
                and _state_facts(runs[head][-1]) == _state_facts(state)
            ):
                merged[head] = current.model_copy(update={"valid_to": state.valid_to})
                merged[index] = None
                runs[head].append(state)
            else:
                head = index
                runs[head] = [state]
    for head, run in runs.items():
        if len(run) > 1:
            current = merged[head]
            assert current is not None
            merged[head] = current.model_copy(
                update={"classification_links": _merged_links(run)}
            )
    return tuple(state for state in merged if state is not None)


class ResolvedVariable(_ResolvedModel):
    register_ref: ResolvedRegister = Field(alias="register")
    slug: str
    provider_key: str
    name: str | None
    definition: str | None
    description: str | None
    operational_definition: str | None
    measurement_unit: str | None
    is_sensitive: bool | None
    is_identifier: bool | None
    states: tuple[ResolvedState, ...]
    aliases: tuple[ResolvedAlias, ...] = ()
    deprecated: bool = False
    source_register: ResolvedRegister | None = None
    source_register_text: str | None = None
    source_label: str | None = None

    _text = field_validator("provider_key")(_require_trimmed)

    @field_validator("name")
    @classmethod
    def _name(cls, value: str | None) -> str | None:
        return _require_trimmed(value) if value is not None else None

    @field_validator("states")
    @classmethod
    def _merged_states(
        cls, value: tuple[ResolvedState, ...]
    ) -> tuple[ResolvedState, ...]:
        # Every producer (formation, committed fixtures) and every revalidation
        # passes here, so lineage, warnings, coverage and the writer all see the
        # merged states.
        return merge_adjacent_states(value)

    @model_validator(mode="after")
    def _resolved_identity_and_states(self) -> Self:
        validate_slug(self.register_ref.provider, "provider")
        validate_slug(self.register_ref.slug, "register")
        validate_slug(self.slug, "variable")
        if not self.states:
            raise ValueError("a resolved variable needs at least one delivery state")
        if self.name is None and any(
            not state.name or not state.name.strip() for state in self.states
        ):
            raise ValueError(
                "a variable without a common name needs positive delivery names"
            )
        previous: dict[tuple[str, str], ResolvedState] = {}
        for state in sorted(
            self.states,
            key=lambda s: (
                s.variant.slug,
                s.value_set_version_label,
                s.period_scope,
                s.valid_from or "",
            ),
        ):
            key = state.variant.slug, state.value_set_version_label
            prior = previous.get(key)
            if prior is not None:
                if prior.variant != state.variant:
                    raise ValueError(
                        f"inconsistent variant definition: {state.variant.slug}"
                    )
                if state.period_scope != prior.period_scope:
                    raise ValueError(
                        "mixed dated and year-independent states in one owner/variant/code version"
                    )
                if state.period_scope == "year_independent":
                    raise ValueError(
                        "duplicate year-independent state in one owner/variant/code version"
                    )
                assert state.valid_from is not None and prior.valid_to is not None
                if state.valid_from <= prior.valid_to:
                    raise ValueError(
                        f"overlapping states in variant: {state.variant.slug}"
                    )
            previous[key] = state
        independent_variants = {
            state.variant.slug
            for state in self.states
            if state.period_scope == "year_independent"
        }
        if independent_variants & {
            state.variant.slug
            for state in self.states
            if state.period_scope == "intervals"
        }:
            raise ValueError(
                "mixed dated and year-independent delivery in one owner/variant"
            )
        if any(
            alias.windows and alias.variant.slug in independent_variants
            for alias in self.aliases
        ):
            raise ValueError(
                "dated alias windows cannot represent year-independent delivery"
            )
        aliases = [
            (alias.variant.slug, alias.delivery_column_name) for alias in self.aliases
        ]
        if len(aliases) != len(set(aliases)):
            raise ValueError("duplicate resolved alias column in one variant")
        return self


def unresolved_variable_flags(variable: ResolvedVariable) -> tuple[str, ...]:
    """Unknown evidence cannot be represented faithfully by the public DB contract."""
    return tuple(
        name
        for name in ("is_sensitive", "is_identifier")
        if getattr(variable, name) is None
    )


# The wording of the per-column window checks `validate_built_db` runs, by
# diagnostic code. Formation reports the same failure before write
# (`column_state_overlaps`).
_COLUMN_STATE_OVERLAPS = {
    "overlapping_distinct_value_sets": (
        "overlapping distinct-value_set state pair(s)",
        "a period resolves to >1 value set",
    ),
    "overlapping_codeless_codebearing_states": (
        "code-less ↔ code-bearing overlapping state pair(s)",
        "a code-less window overlaps a code-bearing window",
    ),
    "overlapping_pooled_explicit_states": (
        "pooled ↔ explicit overlapping state pair(s)",
        "a pooled window overlaps an explicit window",
    ),
}


def column_state_overlap_failure(
    code: str, pairs: int, columns: int, sample: str
) -> str:
    what, why = _COLUMN_STATE_OVERLAPS[code]
    return (
        f"{pairs} {what} on one column across {columns} (variable, column) — "
        f"{why}: {sample}"
    )


# The conflicting pair of each per-column window check `validate_built_db` runs
# on written states, by diagnostic code.
_COLUMN_STATE_CONFLICTS = {
    "overlapping_distinct_value_sets": lambda a, b: (
        a.value_set is not None
        and b.value_set is not None
        and a.value_set != b.value_set
    ),
    "overlapping_codeless_codebearing_states": lambda a, b: (
        (a.value_set is None) != (b.value_set is None)
    ),
    # Reached when case-folded spellings carry different code-list version labels:
    # the pooled span is not cut around the other spelling's explicit year (build
    # case occurrence-pooled-and-explicit-folded-spellings-withhold-their-variable).
    "overlapping_pooled_explicit_states": lambda a, b: a.pooled != b.pooled,
}


def column_state_overlaps(variable: ResolvedVariable) -> tuple[tuple[str, str], ...]:
    """The per-column window checks this variable's states would fail once written.

    One ``(code, message)`` per failed check, in the validator's wording. States
    pair on one variant and one delivery column folded like the validator's
    ``py_lower``, over closed windows, so formation can attribute the failure
    before anything is written.
    """
    fqid = (
        f"{variable.register_ref.provider}/{variable.register_ref.slug}/{variable.slug}"
    )
    by_column: dict[tuple[str, str], list[ResolvedState]] = defaultdict(list)
    for state in variable.states:
        by_column[state.variant.slug, state.delivery_column_name.lower()].append(state)
    # Pairwise like the validator's self-join; one column holds a handful of states.
    overlapping = [
        (column, a, b)
        for (_, column), states in by_column.items()
        for a, b in combinations(states, 2)
        if (
            a.period_scope == b.period_scope == "year_independent"
            and a.value_set_version_label == b.value_set_version_label
        )
        or (
            a.period_scope == b.period_scope == "intervals"
            and a.valid_from is not None
            and a.valid_to is not None
            and b.valid_from is not None
            and b.valid_to is not None
            and a.valid_from <= b.valid_to
            and b.valid_from <= a.valid_to
        )
    ]
    failures = []
    for code, conflict in _COLUMN_STATE_CONFLICTS.items():
        if pairs := [(c, a, b) for c, a, b in overlapping if conflict(a, b)]:
            sample = "; ".join(
                f"{fqid} {a.variant.slug}/{a.delivery_column_name} "
                f"{a.valid_from}..{a.valid_to} {a.value_set_version_label!r} ∩ "
                f"{b.valid_from}..{b.valid_to} {b.value_set_version_label!r}"
                for _, a, b in pairs[:5]
            )
            columns = len({column for column, _, _ in pairs})
            failures.append(
                (code, column_state_overlap_failure(code, len(pairs), columns, sample))
            )
    return tuple(failures)


def validate_resolved_variables(
    variables: tuple[ResolvedVariable, ...],
    *,
    allow_empty: bool = False,
) -> tuple[
    tuple[ResolvedVariable, ...],
    dict[tuple[str, str], ResolvedRegister],
    dict[tuple[str, str, str], ResolvedVariant],
]:
    """Check the whole resolved collection and return its shared identity indexes."""
    variables = TypeAdapter(tuple[ResolvedVariable, ...]).validate_python(
        variables, strict=True
    )
    if not variables and not allow_empty:
        raise ValueError("refusing to publish an empty resolved catalog")
    registers: dict[tuple[str, str], ResolvedRegister] = {}
    variants: dict[tuple[str, str, str], ResolvedVariant] = {}
    variable_keys: set[tuple[str, str, str]] = set()
    for variable in variables:
        register = variable.register_ref
        register_key = (register.provider, register.slug)
        _provider_id_for(register.provider)
        if register_key in registers and registers[register_key] != register:
            raise ValueError(
                f"inconsistent register definition: {'/'.join(register_key)}"
            )
        registers[register_key] = register
        if variable.source_register is not None:
            source = variable.source_register
            source_key = (source.provider, source.slug)
            _provider_id_for(source.provider)
            if source_key in registers and registers[source_key] != source:
                raise ValueError(
                    f"inconsistent source register definition: {'/'.join(source_key)}"
                )
            registers[source_key] = source
        variable_key = (*register_key, variable.slug)
        if unknown_flags := unresolved_variable_flags(variable):
            raise ValueError(
                f"{'/'.join(variable_key)}: unknown flags cannot be materialized "
                f"under the current catalog contract: {', '.join(unknown_flags)}"
            )
        if variable_key in variable_keys:
            raise ValueError(f"duplicate variable FQID: {'/'.join(variable_key)}")
        variable_keys.add(variable_key)
        for variant in (
            *(state.variant for state in variable.states),
            *(alias.variant for alias in variable.aliases),
        ):
            variant_key = (*register_key, variant.slug)
            if variant_key in variants and variants[variant_key] != variant:
                raise ValueError(
                    f"inconsistent variant definition: {'/'.join(variant_key)}"
                )
            variants[variant_key] = variant

    return variables, registers, variants


def _validate_catalog_metadata(
    variables: tuple[ResolvedVariable, ...],
    editions: tuple[ResolvedEdition, ...],
    classifications: tuple[ResolvedClassification, ...],
    registers: dict[tuple[str, str], ResolvedRegister],
    variants: dict[tuple[str, str, str], ResolvedVariant],
) -> None:
    """Check shared definitions, references and canonical code membership."""
    edition_keys = set()
    for edition in editions:
        register = edition.register_ref
        register_key = register.provider, register.slug
        _provider_id_for(register.provider)
        if register_key in registers and registers[register_key] != register:
            raise ValueError("inconsistent edition register definition")
        registers[register_key] = register
        variant_key = (*register_key, edition.variant.slug)
        if variant_key in variants and variants[variant_key] != edition.variant:
            raise ValueError("inconsistent edition variant definition")
        variants[variant_key] = edition.variant
        edition_key = (*variant_key, edition.name)
        if edition_key in edition_keys:
            raise ValueError("duplicate resolved edition")
        edition_keys.add(edition_key)

    by_slug = {item.slug: item for item in classifications}
    if len(by_slug) != len(classifications) or len(
        {item.short_name for item in classifications}
    ) != len(classifications):
        raise ValueError("duplicate classification slug or short name")

    def require_classification(slug: str) -> None:
        if slug not in by_slug:
            raise ValueError(f"unknown classification reference: {slug}")

    for variable in variables:
        domains: list[tuple[ResolvedState | ResolvedAliasWindow, str]] = [
            (state, state.delivery_column_name) for state in variable.states
        ]
        domains.extend(
            (window, alias.delivery_column_name)
            for alias in variable.aliases
            for window in alias.windows
        )
        for state, column in domains:
            for link in state.classification_links:
                require_classification(link.classification)
                conformance = link.conformance
                if conformance is None:
                    continue
                require_classification(conformance.declared_classification)
                book = by_slug[conformance.declared_classification]
                canonical = {code.code for code in book.codes}
                sentinels = {sentinel.code for sentinel in book.sentinel_codes}
                assert state.value_set is not None
                checked = set(conformance.checked_codes)
                observed = {
                    pair
                    for pair in state.value_set.members
                    if pair[0] in checked and pair[0] not in canonical
                }
                scoped_pairs = set()
                context = f"{variable.register_ref.provider}/{variable.register_ref.slug}/{variable.slug} column {column!r} {state.valid_from}..{state.valid_to} book {book.slug!r}"
                for certificate in conformance.scoped_sentinels:
                    if (
                        certificate.classification_sha256
                        != canonical_sha256(book.model_dump(mode="json"))
                        or certificate.delivery_column_name != column
                        or getattr(state, "period_scope", "intervals") != "intervals"
                        or state.valid_from is None
                        or state.valid_to is None
                        or not certificate.valid_from
                        <= state.valid_from
                        <= state.valid_to
                        <= certificate.valid_to
                        or not set(certificate.members) <= observed
                        or any(
                            code == member_code and label != member_label
                            for code, label in certificate.members
                            for member_code, member_label in state.value_set.members
                        )
                    ):
                        raise ValueError(
                            f"scoped sentinel certificate disagrees with source state or canonical book: {context}"
                        )
                    scoped_pairs.update(certificate.members)
                expected = {
                    pair
                    for pair in observed
                    if pair[0] not in sentinels and pair not in scoped_pairs
                }
                if expected != set(conformance.nonconforming_members):
                    raise ValueError(
                        f"conformance disagrees with canonical code membership: {context}; expected nonconforming {sorted(expected)!r}, recorded {sorted(conformance.nonconforming_members)!r}"
                    )
                expected_sentinels = observed - expected
                if expected_sentinels != set(conformance.sentinel_members):
                    raise ValueError(
                        f"conformance disagrees with curated sentinel code membership: {context}"
                    )


def _prepare_classification_succession(
    classifications: tuple[ResolvedClassification, ...],
    edges: tuple[ResolvedClassificationSuccession, ...],
    withheld: Collection[str] = (),
) -> tuple[tuple[ResolvedClassification, ...], dict[str, str]]:
    """Validate the full graph and project its active storage back-pointers.

    A `withheld` book is a known endpoint, so the graph through it is checked
    before its edges are withheld; it is not in the returned order.
    """
    by_slug = {item.slug: item for item in classifications}
    graph = TopologicalSorter({slug: set() for slug in (*by_slug, *withheld)})
    seen: dict[tuple[str, str], str | None] = {}
    predecessors: dict[str, str] = {}
    for edge in edges:
        pair = edge.predecessor, edge.successor
        if pair in seen:
            # Edition chains derive one edge per pair, so a repeat is always a
            # relations.toml edge restating it or another curated edge.
            raise curation_error(
                "relations_invalid",
                "curation/relations.toml [[edge]] type='replaced_by' "
                f"class/{pair[0]} -> class/{pair[1]}: duplicate classification "
                f"succession relation {pair!r} (from {seen[pair]} and "
                f"{edge.note}).",
                "Delete the restated edge: the build derives a succession "
                "between editions of one family from their names.",
            )
        seen[pair] = edge.note
        # Compile refuses a curated edge to an uncompiled book (relations_invalid);
        # derived edges come from the books themselves.
        assert all(s in by_slug or s in withheld for s in pair), pair
        graph.add(edge.successor, edge.predecessor)
        if (
            edge.effective_year is None
            or edge.effective_year <= CLASSIFICATION_SUCCESSION_AS_OF_YEAR
        ):
            # The full graph retains every edge; the schema's single pointer is
            # only its deterministic projection, not a second curation decision.
            predecessors[edge.successor] = min(
                predecessors.get(edge.successor, edge.predecessor), edge.predecessor
            )
    try:
        graph.prepare()
    except CycleError as exc:
        # Derived edition chains run forward in time, so a curated
        # replaced_by edge closes every cycle.
        raise curation_error(
            "relations_invalid",
            "curation/relations.toml [[edge]] type='replaced_by': cyclic "
            "classification succession relation through "
            f"{', '.join(f'class/{slug}' for slug in sorted(set(exc.args[1])))}.",
            "Remove the replaced_by edge that closes the cycle.",
        ) from exc
    ordered = []
    while graph.is_active():
        ready = sorted(graph.get_ready())
        ordered.extend(by_slug[slug] for slug in ready if slug in by_slug)
        graph.done(*ready)
    return tuple(ordered), predecessors
