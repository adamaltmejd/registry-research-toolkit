"""Materialize explicitly resolved, codeless catalog variables.

Identity, canonical text, flags, and finite state windows are curation inputs.
This writer only assigns storage IDs and builds the normal catalog database.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)
from reg_meta.db import (
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR,
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY,
    SCHEMA_VERSION,
    register_py_lower,
)
from reg_meta.fqid import Fqid, validate_slug

from reg_meta_build.db import (
    DDL,
    _populate_fts,
    _provider_id_for,
    publish_db,
    seed_providers,
)
from reg_meta_build.id import mint, mint_canonical_scb
from reg_meta_build.validate import validate_built_db


class _ResolvedModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        populate_by_name=True,
        serialize_by_alias=True,
    )


def _require_trimmed(value: str) -> str:
    if not value or value != value.strip():
        raise ValueError(
            "resolved names, keys, and columns must be nonempty and trimmed"
        )
    return value


class ResolvedRegister(_ResolvedModel):
    provider: str
    slug: str
    name: str

    _name = field_validator("name")(_require_trimmed)

    @model_validator(mode="after")
    def _identity(self) -> Self:
        Fqid.register_fqid(self.provider, self.slug)
        return self


class ResolvedVariant(_ResolvedModel):
    slug: str
    name: str

    _name = field_validator("name")(_require_trimmed)

    @field_validator("slug")
    @classmethod
    def _slug(cls, value: str) -> str:
        validate_slug(value, "register_variant")
        return value


class ResolvedState(_ResolvedModel):
    variant: ResolvedVariant
    valid_from: str
    valid_to: str
    delivery_column_name: str
    data_type: str | None
    data_length: str | None
    operational_definition: str | None
    provenance: str

    _column = field_validator("delivery_column_name", "provenance")(_require_trimmed)

    @field_validator("valid_from", "valid_to")
    @classmethod
    def _finite_date(cls, value: str) -> str:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value or parsed.year == 9999:
            raise ValueError("state bounds must be finite full ISO dates (YYYY-MM-DD)")
        return value

    @model_validator(mode="after")
    def _ordered_bounds(self) -> Self:
        if self.valid_from > self.valid_to:
            raise ValueError("state valid_from must not exceed valid_to")
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
    is_sensitive: bool
    is_identifier: bool
    states: tuple[ResolvedState, ...]

    _text = field_validator("provider_key", "name")(_require_trimmed)

    @model_validator(mode="after")
    def _resolved_identity_and_states(self) -> Self:
        Fqid.binding_fqid(self.register_ref.provider, self.register_ref.slug, self.slug)
        if not self.states:
            raise ValueError("a resolved variable needs at least one finite state")
        previous: dict[str, ResolvedState] = {}
        for state in sorted(self.states, key=lambda s: (s.variant.slug, s.valid_from)):
            prior = previous.get(state.variant.slug)
            if prior is not None:
                if prior.variant != state.variant:
                    raise ValueError(
                        f"inconsistent variant definition: {state.variant.slug}"
                    )
                if state.valid_from <= prior.valid_to:
                    raise ValueError(
                        f"overlapping states in variant: {state.variant.slug}"
                    )
            previous[state.variant.slug] = state
        return self


def _storage_id(provider: str, kind: str, *coordinates: str) -> int:
    # The shipped schema validator reserves separate SCB/non-SCB integer bands.
    allocate = mint_canonical_scb if provider == "scb" else mint
    return allocate("resolved-catalog", kind, provider, *coordinates)


def write_resolved_catalog(
    variables: tuple[ResolvedVariable, ...],
    output: Path,
    *,
    manifest: dict[str, str],
) -> Path:
    """Validate, write, and atomically publish a resolved catalog slice.

    No time, source precedence, slug derivation, or state coalescing is inferred.
    The caller supplies reproducible manifest values; schema-owned keys are fixed.
    """
    variables = TypeAdapter(tuple[ResolvedVariable, ...]).validate_python(
        variables, strict=True
    )
    if not variables:
        raise ValueError("refusing to publish an empty resolved catalog")
    metadata = TypeAdapter(dict[str, str]).validate_python(manifest, strict=True)
    for key, value in {
        "schema_version": SCHEMA_VERSION,
        CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY: str(
            CLASSIFICATION_SUCCESSION_AS_OF_YEAR
        ),
    }.items():
        if key in metadata and metadata[key] != value:
            raise ValueError(
                f"manifest conflicts with catalog {key}: {metadata[key]!r}"
            )
        metadata[key] = value

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
        variable_key = (*register_key, variable.slug)
        if variable_key in variable_keys:
            raise ValueError(f"duplicate variable FQID: {'/'.join(variable_key)}")
        variable_keys.add(variable_key)
        for state in variable.states:
            variant_key = (*register_key, state.variant.slug)
            if variant_key in variants and variants[variant_key] != state.variant:
                raise ValueError(
                    f"inconsistent variant definition: {'/'.join(variant_key)}"
                )
            variants[variant_key] = state.variant

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=f".{output.name}.", dir=output.parent) as temporary:
        staged = Path(temporary) / output.name
        with closing(sqlite3.connect(staged)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            register_py_lower(conn)
            conn.executescript(DDL)
            seed_providers(conn)
            for (provider, slug), register in sorted(registers.items()):
                conn.execute(
                    "INSERT INTO register (register_id, provider_id, name, slug) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        _storage_id(provider, "register", slug),
                        _provider_id_for(provider),
                        register.name,
                        slug,
                    ),
                )
            for (provider, register_slug, slug), variant in sorted(variants.items()):
                conn.execute(
                    "INSERT INTO register_variant "
                    "(register_variant_id, register_id, name, slug) VALUES (?, ?, ?, ?)",
                    (
                        _storage_id(provider, "variant", register_slug, slug),
                        _storage_id(provider, "register", register_slug),
                        variant.name,
                        slug,
                    ),
                )
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
                    "is_sensitive, is_identifier) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                    ),
                )
                for state in sorted(
                    variable.states, key=lambda s: (s.variant.slug, s.valid_from)
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
                    )
                    conn.execute(
                        "INSERT INTO variable_state (state_id, variable_id, "
                        "register_variant_id, valid_from, valid_to, delivery_column_name, "
                        "data_type, data_length, operational_definition, provenance) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                        ),
                    )
                    conn.execute(
                        "INSERT OR IGNORE INTO variable_alias "
                        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, ?, ?)",
                        (variable_id, variant_id, state.delivery_column_name),
                    )
            conn.executemany(
                "INSERT INTO import_manifest (key, value) VALUES (?, ?)",
                sorted(metadata.items()),
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
        publish_db(staged, output)
    return output
