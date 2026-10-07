"""Materialize explicitly resolved catalog variables and code memberships.

Identity, canonical text, flags, and explicit state windows are curation inputs.
This writer only assigns storage IDs and builds the normal catalog database.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from contextlib import closing
from graphlib import CycleError, TopologicalSorter
from itertools import combinations
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Literal, Self

from pydantic import (
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)
from reg_meta.catalog import DataWarning  # noqa: TC002
from reg_meta.db import (
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR,
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY,
    DB_FILENAME,
    default_db_dir,
    register_py_lower,
)
from reg_meta.fqid import Fqid, validate_slug
from reg_meta.source_evidence import canonical_sha256

from reg_meta_build._curation import SentinelCode  # noqa: TC001
from reg_meta_build._resolved_common import (
    _classification_id,
    _require_trimmed,
    _ResolvedDeliveryScope,
    _ResolvedModel,
    _ResolvedWindow,
    _storage_id,
)
from reg_meta_build.data_warnings import write_data_warnings
from reg_meta_build.db import (
    DDL,
    SCHEMA_VERSION,
    _populate_fts,
    _provider_id_for,
    _value_set_hash,
    publish_db,
    seed_providers,
)
from reg_meta_build.derive import derive
from reg_meta_build.id import mint
from reg_meta_build.resolved_metadata import (
    ResolvedMetadata,
    prepare_resolved_metadata,
    write_resolved_metadata,
)
from reg_meta_build.validate import column_state_overlap_failure, validate_built_db

CURATION_TREE_SHA256_KEY = "curation_tree_sha256"


class ResolvedRegister(_ResolvedModel):
    provider: str
    slug: str
    name: str
    purpose: str | None = None

    _name = field_validator("name")(_require_trimmed)

    @model_validator(mode="after")
    def _identity(self) -> Self:
        Fqid.register_fqid(self.provider, self.slug)
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
        return self


class ResolvedScopedSentinels(_ResolvedWindow):
    """Build-time certificate from a checked, finite source sentinel decision."""

    delivery_column_name: str
    classification_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_fingerprints: tuple[
        Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")], ...
    ] = Field(min_length=1)
    members: tuple[tuple[str, str], ...] = Field(min_length=1)
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

    @model_validator(mode="after")
    def _resolved_identity_and_states(self) -> Self:
        Fqid.binding_fqid(self.register_ref.provider, self.register_ref.slug, self.slug)
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


def _write_value_sets(
    conn: sqlite3.Connection,
    variables: tuple[ResolvedVariable, ...],
    classifications: tuple[ResolvedClassification, ...],
) -> dict[ResolvedCodeSet, int]:
    """Store content-shared memberships without provider or validity inference."""
    code_sets = sorted(
        {
            state.value_set
            for variable in variables
            for state in variable.states
            if state.value_set is not None
        }
        | {
            window.value_set
            for variable in variables
            for alias in variable.aliases
            for window in alias.windows
            if window.value_set is not None
        },
        key=lambda code_set: code_set.members,
    )
    pairs = {pair for code_set in code_sets for pair in code_set.members}
    pairs.update(
        (code.code, code.label)
        for classification in classifications
        for code in classification.codes
    )
    code_ids = {pair: _value_code_id(*pair) for pair in sorted(pairs)}
    conn.executemany(
        "INSERT INTO value_code (code_id, code, label) VALUES (?, ?, ?)",
        ((code_id, *pair) for pair, code_id in code_ids.items()),
    )
    set_ids: dict[ResolvedCodeSet, int] = {}
    for code_set in code_sets:
        member_hash = _value_set_hash(list(code_set.members))
        set_id = mint("resolved-catalog", "value-set", member_hash.hex())
        conn.execute(
            "INSERT INTO value_set (value_set_id, member_hash) VALUES (?, ?)",
            (set_id, member_hash),
        )
        conn.executemany(
            "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
            ((set_id, code_ids[pair]) for pair in code_set.members),
        )
        set_ids[code_set] = set_id
    return set_ids


def _value_code_id(code: str, label: str) -> int:
    return mint("resolved-catalog", "value-code", code, label)


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
) -> tuple[tuple[ResolvedClassification, ...], dict[str, str]]:
    """Validate the full graph and project its active storage back-pointers."""
    by_slug = {item.slug: item for item in classifications}
    graph = TopologicalSorter({slug: set() for slug in by_slug})
    seen: set[tuple[str, str]] = set()
    predecessors: dict[str, str] = {}
    for edge in edges:
        pair = edge.predecessor, edge.successor
        if pair in seen:
            raise ValueError(f"duplicate classification succession relation: {pair}")
        seen.add(pair)
        for slug in pair:
            if slug not in by_slug:
                raise ValueError(f"unknown classification succession endpoint: {slug}")
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
        raise ValueError("cyclic classification succession relation") from exc
    ordered = []
    while graph.is_active():
        ready = sorted(graph.get_ready())
        ordered.extend(by_slug[slug] for slug in ready)
        graph.done(*ready)
    return tuple(ordered), predecessors


def _write_editions(
    conn: sqlite3.Connection, editions: tuple[ResolvedEdition, ...]
) -> None:
    for edition in sorted(
        editions,
        key=lambda e: (
            e.register_ref.provider,
            e.register_ref.slug,
            e.variant.slug,
            e.name,
        ),
    ):
        register = edition.register_ref
        edition_id = _storage_id(
            register.provider,
            "edition",
            register.slug,
            edition.variant.slug,
            edition.name,
        )
        conn.execute(
            "INSERT INTO register_version (regver_id, register_variant_id, "
            "registerversionnamn, registerversionbeskrivning, registerversionmatinformation, "
            "registerversion_docstaus, registerversion_forstagodkannandedatum, "
            "registerversion_senastgodkanddatum) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                edition_id,
                _storage_id(
                    register.provider, "variant", register.slug, edition.variant.slug
                ),
                edition.name,
                edition.description,
                edition.measurement_information,
                edition.documentation_status,
                edition.first_approved_at,
                edition.last_approved_at,
            ),
        )
        conn.executemany(
            "INSERT INTO population (regver_id, name, definition, comment, date_range) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                (
                    edition_id,
                    population.name,
                    population.definition,
                    population.comment,
                    population.date_range,
                )
                for population in sorted(edition.populations, key=lambda p: p.name)
            ),
        )
        conn.executemany(
            "INSERT INTO object_type (regver_id, name, definition) VALUES (?, ?, ?)",
            (
                (edition_id, item.name, item.definition)
                for item in sorted(edition.object_types, key=lambda item: item.name)
            ),
        )


def _write_classifications(
    conn: sqlite3.Connection,
    classifications: tuple[ResolvedClassification, ...],
    predecessors: dict[str, str],
) -> None:
    for classification in classifications:
        classification_id = _classification_id(classification.slug)
        conn.execute(
            "INSERT INTO classification (id, short_name, name, name_en, publisher, valid_from, "
            "valid_to, description, url, supersedes_id, code_count, valid_code_count, slug) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                classification_id,
                classification.short_name,
                classification.name,
                classification.name_en,
                classification.publisher,
                classification.valid_from,
                classification.valid_to,
                classification.description,
                classification.url,
                _classification_id(predecessors[classification.slug])
                if classification.slug in predecessors
                else None,
                len(classification.codes),
                len({code.code for code in classification.codes}),
                classification.slug,
            ),
        )
        conn.executemany(
            "INSERT INTO classification_code (classification_id, code_id, level, is_valid) "
            "VALUES (?, ?, ?, 1)",
            (
                (classification_id, _value_code_id(code.code, code.label), code.level)
                for code in sorted(
                    classification.codes, key=lambda c: (c.code, c.label)
                )
            ),
        )


def _write_conformance(
    conn: sqlite3.Connection,
    state_id: int,
    conformance: ResolvedConformance,
    classification: ResolvedClassification,
) -> None:
    checked = len(conformance.checked_codes)
    extensions = set(conformance.nonconforming_members) | set(
        conformance.sentinel_members
    )
    nonconforming = len({code for code, _ in extensions})
    matched = checked - nonconforming
    sentinel_pairs = set(conformance.sentinel_members)
    sentinel_meanings = {
        sentinel.code: sentinel.meaning for sentinel in classification.sentinel_codes
    }
    conn.execute(
        "INSERT INTO classification_conformance (state_id, declared_classification_id, status, "
        "checked_code_count, matched_code_count, nonconforming_code_count, overlap) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            state_id,
            _classification_id(conformance.declared_classification),
            conformance.status,
            checked,
            matched,
            nonconforming,
            matched / checked if checked else 1.0,
        ),
    )
    conn.executemany(
        "INSERT INTO classification_conformance_code "
        "(state_id, declared_classification_id, code_id, member_kind, sentinel_meaning, scoped_sentinels) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            (
                state_id,
                _classification_id(conformance.declared_classification),
                _value_code_id(*pair),
                "sentinel" if pair in sentinel_pairs else "nonstandard",
                sentinel_meanings.get(pair[0]) if pair in sentinel_pairs else None,
                json.dumps(
                    [
                        certificate.model_dump(mode="json")
                        for certificate in conformance.scoped_sentinels
                        if pair in certificate.members
                    ],
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
            for pair in sorted(extensions)
        ),
    )


def write_resolved_catalog(
    variables: tuple[ResolvedVariable, ...],
    output: Path,
    *,
    manifest: dict[str, str],
    diagnostic: bool = False,
    scoped: bool = False,
    corpus: bool = False,
    parent_registers: tuple[ResolvedRegister, ...] = (),
    parent_variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...] = (),
    editions: tuple[ResolvedEdition, ...] = (),
    classifications: tuple[ResolvedClassification, ...] = (),
    classification_successions: tuple[ResolvedClassificationSuccession, ...] = (),
    metadata: ResolvedMetadata | None = None,
    data_warnings: tuple[DataWarning, ...] = (),
) -> Path:
    """Validate and atomically place a strict catalog or create-only diagnostic.

    No time, source precedence, slug derivation, or state coalescing is inferred.
    The caller supplies reproducible manifest values; schema-owned keys are fixed.
    Both modes run the same contract and structural checks. Diagnostic and
    register-scoped artifacts are marked incomplete/nonpublishable and can never
    replace an existing file.
    Independently resolved registers and (register, variant) pairs remain present
    even when their variable states are withheld. Shared definitions must agree.
    The complete pipeline additionally requires corpus safeguards before strict
    publication; partial writer fixtures leave those volume expectations disabled.
    """
    diagnostic = TypeAdapter(bool).validate_python(diagnostic, strict=True)
    partial = diagnostic or TypeAdapter(bool).validate_python(scoped, strict=True)
    variables, registers, variants = validate_resolved_variables(
        variables, allow_empty=diagnostic
    )
    parent_registers = TypeAdapter(tuple[ResolvedRegister, ...]).validate_python(
        parent_registers, strict=True
    )
    parent_variants = TypeAdapter(
        tuple[tuple[ResolvedRegister, ResolvedVariant], ...]
    ).validate_python(parent_variants, strict=True)
    for register in (*parent_registers, *(r for r, _ in parent_variants)):
        key = register.provider, register.slug
        _provider_id_for(register.provider)
        if key in registers and registers[key] != register:
            raise ValueError(f"inconsistent resolved parent register: {key!r}")
        registers[key] = register
    for register, variant in parent_variants:
        key = register.provider, register.slug, variant.slug
        if key in variants and variants[key] != variant:
            raise ValueError(f"inconsistent resolved parent variant: {key!r}")
        variants[key] = variant
    editions = TypeAdapter(tuple[ResolvedEdition, ...]).validate_python(
        editions, strict=True
    )
    classifications = TypeAdapter(tuple[ResolvedClassification, ...]).validate_python(
        classifications, strict=True
    )
    _validate_catalog_metadata(
        variables, editions, classifications, registers, variants
    )
    classification_successions = TypeAdapter(
        tuple[ResolvedClassificationSuccession, ...]
    ).validate_python(classification_successions, strict=True)
    classifications, classification_predecessors = _prepare_classification_succession(
        classifications, classification_successions
    )
    metadata_rows = prepare_resolved_metadata(
        ResolvedMetadata() if metadata is None else metadata,
        variables,
        registers,
        variants,
        classifications,
    )
    import_metadata = TypeAdapter(dict[str, str]).validate_python(manifest, strict=True)
    for key in (CURATION_TREE_SHA256_KEY,):
        if (value := import_metadata.get(key)) is not None and (
            len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
        ):
            raise ValueError(f"manifest {key} must be a lowercase SHA-256 digest")
    for key, value in {
        "schema_version": SCHEMA_VERSION,
        "catalog_artifact_kind": "diagnostic" if diagnostic else "catalog",
        "catalog_publishable": "false" if partial else "true",
        "catalog_completeness": "incomplete" if partial else "complete",
        CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY: str(
            CLASSIFICATION_SUCCESSION_AS_OF_YEAR
        ),
    }.items():
        if key in import_metadata and import_metadata[key] != value:
            raise ValueError(
                f"manifest conflicts with catalog {key}: {import_metadata[key]!r}"
            )
        import_metadata[key] = value

    captured_revision = None
    if partial:
        import_metadata.pop("builder_commit", None)
        import_metadata.pop("generation_id", None)
    else:
        from .artifact_identity import builder_commit, generation_id

        if corpus or "builder_commit" not in import_metadata:
            revision = builder_commit()
            if "builder_commit" not in import_metadata:
                captured_revision = revision
                import_metadata["builder_commit"] = revision
        expected_generation = generation_id(import_metadata)
        if (
            "generation_id" in import_metadata
            and import_metadata["generation_id"] != expected_generation
        ):
            raise ValueError(
                "manifest generation_id conflicts with canonical semantic inputs"
            )
        import_metadata["generation_id"] = expected_generation

    output = Path(output)
    if partial and (
        output.exists()
        or output.is_symlink()
        or output.resolve() == (default_db_dir() / DB_FILENAME).resolve()
    ):
        raise ValueError(
            "diagnostic or register-scoped output must be a new explicit path separate from the active catalog"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=f".{output.name}.", dir=output.parent) as temporary:
        staged = Path(temporary) / output.name
        with closing(sqlite3.connect(staged)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            register_py_lower(conn)
            conn.executescript(DDL)
            seed_providers(conn)
            value_set_ids = _write_value_sets(conn, variables, classifications)
            _write_classifications(conn, classifications, classification_predecessors)
            classifications_by_slug = {book.slug: book for book in classifications}
            conn.executemany(
                "INSERT INTO classification_replaced_by "
                "(predecessor_slug, successor_slug, effective_year, note) VALUES (?, ?, ?, ?)",
                (
                    (edge.predecessor, edge.successor, edge.effective_year, edge.note)
                    for edge in sorted(
                        classification_successions,
                        key=lambda edge: (edge.predecessor, edge.successor),
                    )
                ),
            )
            for (provider, slug), register in sorted(registers.items()):
                conn.execute(
                    "INSERT INTO register (register_id, provider_id, name, slug, purpose) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        _storage_id(provider, "register", slug),
                        _provider_id_for(provider),
                        register.name,
                        slug,
                        register.purpose,
                    ),
                )
            for (provider, register_slug, slug), variant in sorted(variants.items()):
                conn.execute(
                    "INSERT INTO register_variant "
                    "(register_variant_id, register_id, name, slug, description, display_group, "
                    "panel_entity_key, panel_time_key, panel_time_grain) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        _storage_id(provider, "variant", register_slug, slug),
                        _storage_id(provider, "register", register_slug),
                        variant.name,
                        slug,
                        variant.description,
                        variant.display_group,
                        json.dumps(variant.panel_entity_key)
                        if isinstance(variant.panel_entity_key, tuple)
                        else variant.panel_entity_key,
                        json.dumps(variant.panel_time_key)
                        if isinstance(variant.panel_time_key, tuple)
                        else variant.panel_time_key,
                        variant.panel_time_grain,
                    ),
                )
            _write_editions(conn, editions)
            for variable in sorted(
                variables,
                key=lambda v: (v.register_ref.provider, v.register_ref.slug, v.slug),
            ):
                provider, register_slug = (
                    variable.register_ref.provider,
                    variable.register_ref.slug,
                )
                variable_id = _storage_id(
                    provider, "variable", register_slug, variable.slug
                )
                conn.execute(
                    "INSERT INTO variable (variable_id, register_id, provider_key, slug, "
                    "name, definition, description, operational_definition, measurement_unit, "
                    "is_sensitive, is_identifier, deprecated, source_register_id, source_register_text, "
                    "source_label) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        variable_id,
                        _storage_id(provider, "register", register_slug),
                        variable.provider_key,
                        variable.slug,
                        variable.name,
                        variable.definition,
                        variable.description,
                        variable.operational_definition,
                        variable.measurement_unit,
                        variable.is_sensitive,
                        variable.is_identifier,
                        variable.deprecated,
                        _storage_id(
                            variable.source_register.provider,
                            "register",
                            variable.source_register.slug,
                        )
                        if variable.source_register is not None
                        else None,
                        variable.source_register_text,
                        variable.source_label,
                    ),
                )
                for state in sorted(
                    variable.states,
                    key=lambda s: (
                        s.variant.slug,
                        s.period_scope,
                        s.valid_from or "",
                        s.value_set_version_label,
                    ),
                ):
                    variant_id = _storage_id(
                        provider, "variant", register_slug, state.variant.slug
                    )
                    state_coordinate = (
                        state.valid_from
                        if state.period_scope == "intervals"
                        else "year_independent"
                    )
                    assert state_coordinate is not None
                    state_id = _storage_id(
                        provider,
                        "state",
                        register_slug,
                        variable.slug,
                        state.variant.slug,
                        state_coordinate,
                        state.value_set_version_label,
                    )
                    conn.execute(
                        "INSERT INTO variable_state (state_id, variable_id, "
                        "register_variant_id, valid_from, valid_to, delivery_column_name, "
                        "data_type, data_length, operational_definition, provenance, pooled, "
                        "value_set_id, value_set_version_label, source_register_text, period_scope, definition, measurement_unit, name, description) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            state_id,
                            variable_id,
                            variant_id,
                            state.valid_from,
                            state.valid_to,
                            state.delivery_column_name,
                            state.data_type,
                            state.data_length,
                            state.operational_definition,
                            state.provenance,
                            int(state.pooled),
                            value_set_ids[state.value_set]
                            if state.value_set is not None
                            else None,
                            state.value_set_version_label,
                            state.source_register_text,
                            state.period_scope,
                            state.definition,
                            state.measurement_unit,
                            state.name,
                            state.description,
                        ),
                    )
                    for link in state.classification_links:
                        conn.execute(
                            "INSERT INTO state_classification (state_id, classification_id, provenance) VALUES (?, ?, ?)",
                            (
                                state_id,
                                _classification_id(link.classification),
                                link.provenance,
                            ),
                        )
                        if link.conformance is not None:
                            _write_conformance(
                                conn,
                                state_id,
                                link.conformance,
                                classifications_by_slug[link.classification],
                            )
                    conn.execute(
                        "INSERT OR IGNORE INTO variable_alias "
                        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, ?, ?)",
                        (variable_id, variant_id, state.delivery_column_name),
                    )
                for alias in sorted(
                    variable.aliases,
                    key=lambda a: (a.variant.slug, a.delivery_column_name),
                ):
                    variant_id = _storage_id(
                        provider, "variant", register_slug, alias.variant.slug
                    )
                    conn.execute(
                        "INSERT OR IGNORE INTO variable_alias "
                        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, ?, ?)",
                        (variable_id, variant_id, alias.delivery_column_name),
                    )
                    conn.executemany(
                        "INSERT INTO variable_alias_window "
                        "(variable_id, register_variant_id, delivery_column_name, valid_from, valid_to, provenance, column_metadata, data_type, data_length, operational_definition, source_register_text, coding_metadata, value_set_id, value_set_version_label, definition, measurement_unit, name, description) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            (
                                variable_id,
                                variant_id,
                                alias.delivery_column_name,
                                window.valid_from,
                                window.valid_to,
                                window.provenance,
                                window.column_metadata,
                                window.data_type,
                                window.data_length,
                                window.operational_definition,
                                window.source_register_text,
                                window.coding_metadata,
                                value_set_ids[window.value_set]
                                if window.value_set is not None
                                else None,
                                window.value_set_version_label,
                                window.definition,
                                window.measurement_unit,
                                window.name,
                                window.description,
                            )
                            for window in sorted(
                                alias.windows, key=lambda w: w.valid_from
                            )
                        ),
                    )
                    for window in sorted(alias.windows, key=lambda w: w.valid_from):
                        for link in window.classification_links:
                            conn.execute(
                                "INSERT INTO alias_window_classification "
                                "(variable_id, register_variant_id, delivery_column_name, valid_from, classification_id, provenance, conformance) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (
                                    variable_id,
                                    variant_id,
                                    alias.delivery_column_name,
                                    window.valid_from,
                                    _classification_id(link.classification),
                                    link.provenance,
                                    link.conformance.model_dump_json()
                                    if link.conformance is not None
                                    else None,
                                ),
                            )
            write_data_warnings(conn, data_warnings, demote_missing_variables=True)
            write_resolved_metadata(conn, metadata_rows)
            conn.execute(
                "INSERT INTO code_variable_map (code_id, variable_id) "
                "SELECT DISTINCT member.code_id, state.variable_id "
                "FROM variable_state state JOIN value_set_member member "
                "ON state.value_set_id = member.value_set_id "
                "UNION SELECT DISTINCT member.code_id, alias.variable_id "
                "FROM variable_alias_window alias JOIN value_set_member member "
                "ON alias.value_set_id = member.value_set_id"
            )
            conn.execute(
                "UPDATE value_code SET mapping_count = ("
                "SELECT COUNT(*) FROM code_variable_map "
                "WHERE code_id = value_code.code_id)"
            )
            conn.executemany(
                "INSERT INTO import_manifest (key, value) VALUES (?, ?)",
                sorted(import_metadata.items()),
            )
            for table in (
                "variable_alias_build",
                "variable_instance",
                "classification_candidate",
                "unika_summary",
            ):
                conn.execute(f"DROP TABLE {table}")
            conn.commit()
            derive(conn)
            _populate_fts(conn)
            # Last write: readers plan with these statistics. ANALYZE is a pure
            # function of the table contents, so rebuilds stay byte-identical.
            conn.execute("ANALYZE")
            conn.commit()
            conn.execute("VACUUM")
        validation = validate_built_db(staged, corpus=corpus)
        if not validation.passed:
            raise ValueError(
                "resolved catalog validation failed: " + "; ".join(validation.failures)
            )
        if partial:
            # Create-only placement is atomic and cannot clobber a normal catalog
            # even if another process creates the destination during the build.
            output.hardlink_to(staged)
        else:
            if (corpus or captured_revision is not None) and (
                builder_commit() != import_metadata["builder_commit"]
            ):
                raise ValueError("Builder revision changed during compilation")
            publish_db(staged, output)
    return output
