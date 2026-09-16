"""Materialize explicitly resolved catalog variables and code memberships.

Identity, canonical text, flags, and finite state windows are curation inputs.
This writer only assigns storage IDs and builds the normal catalog database.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal, Self

from pydantic import (
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)
from reg_meta.db import (
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR,
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY,
    DB_FILENAME,
    SCHEMA_VERSION,
    default_db_dir,
    register_py_lower,
)
from reg_meta.fqid import Fqid, validate_slug

from reg_meta_build._resolved_common import (
    _classification_id,
    _require_trimmed,
    _ResolvedModel,
    _ResolvedWindow,
    _storage_id,
)
from reg_meta_build.db import (
    DDL,
    _populate_fts,
    _provider_id_for,
    _value_set_hash,
    publish_db,
    seed_providers,
)
from reg_meta_build.id import mint
from reg_meta_build.resolved_metadata import (
    ResolvedMetadata,
    prepare_resolved_metadata,
    write_resolved_metadata,
)
from reg_meta_build.validate import validate_built_db


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
        validate_slug(value, "register_variant")
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
    supersedes: str | None = None
    codes: tuple[ResolvedClassificationCode, ...] = Field(min_length=1)

    _names = field_validator("name", "short_name")(_require_trimmed)

    @field_validator("slug", "supersedes")
    @classmethod
    def _slugs(cls, value: str | None) -> str | None:
        if value is not None:
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
        return self


class ResolvedConformance(_ResolvedModel):
    """The common resolver's explicit conformance decision, including omissions."""

    declared_classification: str
    status: Literal["kept", "severed"]
    checked_codes: tuple[str, ...]
    nonconforming_members: tuple[tuple[str, str], ...] = ()

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


class ResolvedState(_ResolvedWindow):
    variant: ResolvedVariant
    delivery_column_name: str
    data_type: str | None
    data_length: str | None
    operational_definition: str | None
    provenance: str | None
    source_register_text: str | None = None
    value_set: ResolvedCodeSet | None = None
    value_set_version_label: str = ""
    classification: str | None = None
    conformance: ResolvedConformance | None = None

    _column = field_validator("delivery_column_name")(_require_trimmed)

    @model_validator(mode="after")
    def _classification_contract(self) -> Self:
        if self.classification is not None:
            validate_slug(self.classification, "classification")
        if self.conformance is not None:
            expected = (
                self.conformance.declared_classification
                if self.conformance.status == "kept"
                else None
            )
            if self.classification != expected or self.value_set is None:
                raise ValueError(
                    "state classification and conformance decision disagree"
                )
            members = set(self.value_set.members)
            checked = set(self.conformance.checked_codes)
            if not checked <= {code for code, _ in members}:
                raise ValueError(
                    "conformance checks a code outside the state value set"
                )
            nonconforming = set(self.conformance.nonconforming_members)
            if (
                not nonconforming <= members
                or not {code for code, _ in nonconforming} <= checked
            ):
                raise ValueError("nonconforming members must be checked state members")
        return self


class ResolvedAliasWindow(_ResolvedWindow):
    provenance: str | None = None


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
    name: str
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

    _text = field_validator("provider_key", "name")(_require_trimmed)

    @model_validator(mode="after")
    def _resolved_identity_and_states(self) -> Self:
        Fqid.binding_fqid(self.register_ref.provider, self.register_ref.slug, self.slug)
        if not self.states:
            raise ValueError("a resolved variable needs at least one finite state")
        previous: dict[tuple[str, str], ResolvedState] = {}
        for state in sorted(
            self.states,
            key=lambda s: (s.variant.slug, s.value_set_version_label, s.valid_from),
        ):
            key = state.variant.slug, state.value_set_version_label
            prior = previous.get(key)
            if prior is not None:
                if prior.variant != state.variant:
                    raise ValueError(
                        f"inconsistent variant definition: {state.variant.slug}"
                    )
                if state.valid_from <= prior.valid_to:
                    raise ValueError(
                        f"overlapping states in variant: {state.variant.slug}"
                    )
            previous[key] = state
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


def validate_resolved_variables(
    variables: tuple[ResolvedVariable, ...],
) -> tuple[
    tuple[ResolvedVariable, ...],
    dict[tuple[str, str], ResolvedRegister],
    dict[tuple[str, str, str], ResolvedVariant],
]:
    """Check the whole resolved collection and return its shared identity indexes."""
    variables = TypeAdapter(tuple[ResolvedVariable, ...]).validate_python(
        variables, strict=True
    )
    if not variables:
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
) -> tuple[ResolvedClassification, ...]:
    """Check shared references and order classification predecessors before users."""
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
    ordered: dict[str, ResolvedClassification] = {}
    active: set[str] = set()

    def include(slug: str) -> None:
        if slug in ordered:
            return
        if slug not in by_slug:
            raise ValueError(f"unknown classification reference: {slug}")
        if slug in active:
            raise ValueError("cyclic classification supersedes relation")
        active.add(slug)
        item = by_slug[slug]
        if item.supersedes is not None:
            include(item.supersedes)
        active.remove(slug)
        ordered[slug] = item

    for slug in sorted(by_slug):
        include(slug)
    for variable in variables:
        for state in variable.states:
            if state.classification is not None:
                include(state.classification)
            conformance = state.conformance
            if conformance is None:
                continue
            include(conformance.declared_classification)
            canonical = {
                code.code for code in by_slug[conformance.declared_classification].codes
            }
            assert state.value_set is not None
            checked = set(conformance.checked_codes)
            expected = {
                pair
                for pair in state.value_set.members
                if pair[0] in checked and pair[0] not in canonical
            }
            if expected != set(conformance.nonconforming_members):
                raise ValueError("conformance disagrees with canonical code membership")
    return tuple(ordered.values())


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
    conn: sqlite3.Connection, classifications: tuple[ResolvedClassification, ...]
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
                _classification_id(classification.supersedes)
                if classification.supersedes is not None
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
    conn: sqlite3.Connection, state_id: int, conformance: ResolvedConformance
) -> None:
    checked = len(conformance.checked_codes)
    nonconforming = len({code for code, _ in conformance.nonconforming_members})
    matched = checked - nonconforming
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
        "INSERT INTO classification_conformance_code (state_id, code_id) VALUES (?, ?)",
        (
            (state_id, _value_code_id(*pair))
            for pair in sorted(conformance.nonconforming_members)
        ),
    )


def write_resolved_catalog(
    variables: tuple[ResolvedVariable, ...],
    output: Path,
    *,
    manifest: dict[str, str],
    diagnostic: bool = False,
    editions: tuple[ResolvedEdition, ...] = (),
    classifications: tuple[ResolvedClassification, ...] = (),
    classification_successions: tuple[ResolvedClassificationSuccession, ...] = (),
    metadata: ResolvedMetadata | None = None,
) -> Path:
    """Validate and atomically place a strict catalog or create-only diagnostic.

    No time, source precedence, slug derivation, or state coalescing is inferred.
    The caller supplies reproducible manifest values; schema-owned keys are fixed.
    Both modes run the same contract and structural checks. Diagnostic artifacts
    are marked incomplete/nonpublishable and can never replace an existing file.
    """
    diagnostic = TypeAdapter(bool).validate_python(diagnostic, strict=True)
    variables, registers, variants = validate_resolved_variables(variables)
    editions = TypeAdapter(tuple[ResolvedEdition, ...]).validate_python(
        editions, strict=True
    )
    classifications = TypeAdapter(tuple[ResolvedClassification, ...]).validate_python(
        classifications, strict=True
    )
    classifications = _validate_catalog_metadata(
        variables, editions, classifications, registers, variants
    )
    classification_successions = TypeAdapter(
        tuple[ResolvedClassificationSuccession, ...]
    ).validate_python(classification_successions, strict=True)
    succession_graph = TopologicalSorter()
    for edge in classification_successions:
        succession_graph.add(edge.successor, edge.predecessor)
    try:
        succession_graph.prepare()
    except CycleError as exc:
        raise ValueError("cyclic classification succession relation") from exc
    metadata_rows = prepare_resolved_metadata(
        ResolvedMetadata() if metadata is None else metadata,
        variables,
        registers,
        variants,
        classifications,
    )
    import_metadata = TypeAdapter(dict[str, str]).validate_python(manifest, strict=True)
    for key, value in {
        "schema_version": SCHEMA_VERSION,
        "catalog_artifact_kind": "diagnostic" if diagnostic else "catalog",
        "catalog_publishable": "false" if diagnostic else "true",
        "catalog_completeness": "incomplete" if diagnostic else "complete",
        CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY: str(
            CLASSIFICATION_SUCCESSION_AS_OF_YEAR
        ),
    }.items():
        if key in import_metadata and import_metadata[key] != value:
            raise ValueError(
                f"manifest conflicts with catalog {key}: {import_metadata[key]!r}"
            )
        import_metadata[key] = value

    output = Path(output)
    if diagnostic and (
        output.exists()
        or output.is_symlink()
        or output.resolve() == (default_db_dir() / DB_FILENAME).resolve()
    ):
        raise ValueError(
            "diagnostic output must be a new explicit path separate from the active catalog"
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
            _write_classifications(conn, classifications)
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
                        s.valid_from,
                        s.value_set_version_label,
                    ),
                ):
                    variant_id = _storage_id(
                        provider, "variant", register_slug, state.variant.slug
                    )
                    state_id = _storage_id(
                        provider,
                        "state",
                        register_slug,
                        variable.slug,
                        state.variant.slug,
                        state.valid_from,
                        state.value_set_version_label,
                    )
                    conn.execute(
                        "INSERT INTO variable_state (state_id, variable_id, "
                        "register_variant_id, valid_from, valid_to, delivery_column_name, "
                        "data_type, data_length, operational_definition, provenance, "
                        "value_set_id, value_set_version_label, source_register_text, classification_id) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                            value_set_ids[state.value_set]
                            if state.value_set is not None
                            else None,
                            state.value_set_version_label,
                            state.source_register_text,
                            _classification_id(state.classification)
                            if state.classification is not None
                            else None,
                        ),
                    )
                    if state.conformance is not None:
                        _write_conformance(conn, state_id, state.conformance)
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
                        "(variable_id, register_variant_id, delivery_column_name, valid_from, valid_to, provenance) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            (
                                variable_id,
                                variant_id,
                                alias.delivery_column_name,
                                window.valid_from,
                                window.valid_to,
                                window.provenance,
                            )
                            for window in sorted(
                                alias.windows, key=lambda w: w.valid_from
                            )
                        ),
                    )
            write_resolved_metadata(conn, metadata_rows)
            conn.execute(
                "INSERT INTO code_variable_map (code_id, variable_id) "
                "SELECT DISTINCT member.code_id, state.variable_id "
                "FROM variable_state state JOIN value_set_member member "
                "ON state.value_set_id = member.value_set_id"
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
            _populate_fts(conn)
            conn.commit()
            conn.execute("VACUUM")
        validation = validate_built_db(staged, corpus=False)
        if not validation.passed:
            raise ValueError(
                "resolved catalog validation failed: " + "; ".join(validation.failures)
            )
        if diagnostic:
            # Create-only placement is atomic and cannot clobber a normal catalog
            # even if another process creates the destination during the build.
            output.hardlink_to(staged)
        else:
            publish_db(staged, output)
    return output
