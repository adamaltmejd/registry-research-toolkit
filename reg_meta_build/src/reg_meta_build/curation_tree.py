"""Read the tracked curation tree under ``reg_meta_build/curation/``.

``classifications/<short_name>.toml`` holds one classification: its metadata and
sentinels in ``[classification]``, and in ``[binding]`` the value-set labels behind
the label binding rule plus the curated ``[[binding.variable]]`` bindings.
Family names and exact family aliases are curated with each member's metadata.
``registers/<provider>/<slug>.toml`` (or one family directory below the provider)
owns register-scoped declarations. ``classification_groups.toml``,
``relations.toml``, ``tags.toml`` and ``lineage.toml`` stay at the root as global
files. The reader returns typed, validated entries and never touches prepared
data. Files enumerate in sorted path order, and every error names its file and
entry index.
"""

from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import validate_slug

from ._curation import (
    SentinelCode,
    curation_error,
    load_sentinel_codes,
    repo_curation_dir,
)
from ._resolved_common import _require_trimmed
from .fqid_slugs import (
    load_lineage_config,
)
from .normalization import normalize_text
from .relations import load_relations
from .resolved_metadata import _variable
from .tags import load_tags

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .fqid_slugs import LineageConfig
    from .relations import CuratedRelations
    from .tags import CuratedTag

CLASSIFICATIONS_DIR = "classifications"
_CODE = "classification_curation_invalid"


class _CurationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ClassificationMetadata(_CurationModel):
    """``[classification]``: one codebook and its canonical code CSV."""

    short_name: str
    aliases: Annotated[tuple[str, ...], Field(strict=False)] = ()
    family: str | None = None
    family_aliases: Annotated[tuple[str, ...], Field(strict=False)] = ()
    slug: str
    name: str
    name_en: str | None = None
    publisher: str | None = None
    valid_from: int | None = None
    valid_to: int | None = None
    description: str | None = None
    url: str | None = None
    # Relative to `input_data/classifications/`; SOS books sit under `sos/`.
    codes_file: str
    sentinel_codes: tuple[SentinelCode, ...] = ()

    _names = field_validator("short_name", "name")(_require_trimmed)

    @field_validator("aliases", "family_aliases")
    @classmethod
    def _aliases(cls, aliases: tuple[str, ...]) -> tuple[str, ...]:
        if any(
            not alias or alias != normalize_text(alias, multiline=True)
            for alias in aliases
        ):
            raise ValueError("classification aliases must be non-empty normalized text")
        if len(aliases) != len(set(aliases)):
            raise ValueError("classification aliases must be unique")
        return aliases

    @field_validator("family")
    @classmethod
    def _family(cls, family: str | None) -> str | None:
        if family is not None:
            validate_slug(family, "classification family")
        return family

    @model_validator(mode="after")
    def _family_alias_ownership(self) -> ClassificationMetadata:
        if self.family_aliases and self.family is None:
            raise ValueError("family aliases require a family")
        return self

    @field_validator("slug")
    @classmethod
    def _slug(cls, value: str) -> str:
        validate_slug(value, "classification")
        return value

    @field_validator("codes_file")
    @classmethod
    def _codes_file(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != value
            or path.suffix != ".csv"
        ):
            raise ValueError("must be a normalized relative .csv path")
        return value

    @field_validator("sentinel_codes", mode="before")
    @classmethod
    def _sentinels(cls, value: object, info: ValidationInfo) -> object:
        return load_sentinel_codes(
            value, classification=(info.context or {}).get("file", "?"), code=_CODE
        )


class BoundVariable(_CurationModel):
    """``[[binding.variable]]``: a variable whose inline codes use this book."""

    variable: str
    note: str | None = None

    _fqid = field_validator("variable")(_variable)
    _note = field_validator("note")(_require_trimmed)


class ClassificationBinding(_CurationModel):
    """``[binding]``: how source states bind to the classification."""

    # The provider whose value-set version labels carry the classification.
    label_source: str | None = None
    value_set_labels: Annotated[tuple[str, ...], Field(strict=False)] = ()
    variable: Annotated[tuple[BoundVariable, ...], Field(strict=False)] = ()

    @field_validator("value_set_labels")
    @classmethod
    def _labels(cls, labels: tuple[str, ...]) -> tuple[str, ...]:
        for label in labels:
            if not label or label != normalize_text(label):
                raise ValueError(
                    f"label {label!r} is not normalized; write "
                    f"{normalize_text(label)!r}"
                )
        return labels


class CuratedClassification(_CurationModel):
    """One ``classifications/<short_name>.toml`` file."""

    classification: ClassificationMetadata
    binding: ClassificationBinding = ClassificationBinding()


class ClassificationFamily(_CurationModel):
    key: str
    members: Annotated[tuple[str, ...], Field(strict=False)]
    aliases: Annotated[tuple[str, ...], Field(strict=False)] = ()

    _key = field_validator("key")(_require_trimmed)

    @model_validator(mode="after")
    def _valid(self) -> ClassificationFamily:
        if len(self.members) < 2 or len(self.members) != len(set(self.members)):
            raise ValueError("family members must name at least two distinct slugs")
        for member in self.members:
            validate_slug(member, "classification")
        if len(self.aliases) != len(set(self.aliases)):
            raise ValueError("family aliases must be unique")
        if any(
            not alias or alias != normalize_text(alias, multiline=True)
            for alias in self.aliases
        ):
            raise ValueError("family aliases must be non-empty normalized text")
        return self


class ClassificationFamilies(_CurationModel):
    family: Annotated[tuple[ClassificationFamily, ...], Field(strict=False)] = ()


class RegisterIdentity(_CurationModel):
    """The coordinate asserted by a register file's path."""

    provider: str
    slug: str
    native_id: str | None = None
    name: str | None = None
    steward_table_prefixes: tuple[str, ...] = ()
    source_labels: list[str] = Field(default_factory=list)

    _provider = field_validator("provider")(_require_trimmed)
    _slug = field_validator("slug")(_require_trimmed)
    _native_id = field_validator("native_id")(_require_trimmed)
    _name = field_validator("name")(_require_trimmed)

    @field_validator("steward_table_prefixes", mode="before")
    @classmethod
    def _prefix_list(cls, value: object) -> object:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("steward_table_prefixes")
    @classmethod
    def _literal_prefixes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not prefix or prefix != prefix.strip() for prefix in value):
            raise ValueError("steward table prefixes must be non-empty literal names")
        if len(value) != len(set(value)):
            raise ValueError("steward table prefixes must be unique")
        return value

    @field_validator("source_labels")
    @classmethod
    def _source_labels(cls, labels: list[str]) -> list[str]:
        return [_require_trimmed(label) for label in labels]

    @field_validator("slug")
    @classmethod
    def _valid_slug(cls, value: str) -> str:
        validate_slug(value, "register")
        return value


class RegisterVariantSlug(_CurationModel):
    native_id: str
    slug: str
    display_group: str | None = None
    panel_entity_key: str | list[str] | None = None
    panel_time_key: str | list[str] | None = None
    panel_time_grain: str | None = None
    deprecated: bool = False
    replaced_by: str | None = None

    @field_validator("native_id")
    @classmethod
    def _native_id(cls, value: str) -> str:
        from .fqid_slugs import _parse_variant_id

        try:
            _parse_variant_id(value)
        except RegMetaError as exc:
            raise ValueError(exc.message) from exc
        return value


class RegisterVariableSlug(_CurationModel):
    native_id: str
    slug: str | None = None
    replaced_by: str | None = None
    deprecated: bool = False


class ErrataVersionEntry(_CurationModel):
    variant: str
    name: str
    evidence: str
    noted: str


class ErrataEditionPeriodEntry(_CurationModel):
    variant: str
    name: str
    valid_from: str
    valid_to: str
    evidence: str
    noted: str

    @model_validator(mode="after")
    def _valid_period(self) -> ErrataEditionPeriodEntry:
        for field in ("variant", "name", "evidence"):
            if not getattr(self, field).strip():
                raise ValueError(f"{field} must not be blank")
        for field in ("valid_from", "valid_to", "noted"):
            value = getattr(self, field)
            try:
                parsed = date.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(f"{field} must be an ISO date") from exc
            if parsed.isoformat() != value or (
                field != "noted" and parsed.year == 9999
            ):
                raise ValueError(f"{field} must be a canonical finite ISO date")
        if self.valid_to < self.valid_from:
            raise ValueError("valid_to must not precede valid_from")
        return self


class ErrataDeliveredEntry(_CurationModel):
    variant: str
    column: str
    versions: list[str]
    evidence: str
    noted: str
    upstream: str | None = None
    native_variable_id: int | None = None


class ErrataColumnEntry(_CurationModel):
    variant: str
    column: str
    name: str
    definition: str
    data_type: str | None = None
    classification: str | None = None
    is_identifier: bool | None = None
    is_sensitive: bool | None = None
    versions: list[str] | None = None
    all_versions: bool | None = None
    holdings_period: str | None = None
    source: str
    evidence: str
    noted: str


class ErrataCuration(_CurationModel):
    delivered: list[ErrataDeliveredEntry] = Field(default_factory=list)
    column: list[ErrataColumnEntry] = Field(default_factory=list)
    version: list[ErrataVersionEntry] = Field(default_factory=list)
    edition_period: list[ErrataEditionPeriodEntry] = Field(default_factory=list)


class EnrichmentDescriptionEntry(_CurationModel):
    register_fqid: str = Field(validation_alias="register")
    variable: str
    description: str
    provenance: str = ""


class EnrichmentAliasEntry(_CurationModel):
    register_fqid: str = Field(validation_alias="register")
    variable: str
    delivery_column: str
    provenance: str = ""


class EnrichmentCuration(_CurationModel):
    description: list[EnrichmentDescriptionEntry] = Field(default_factory=list)
    alias: list[EnrichmentAliasEntry] = Field(default_factory=list)


class GroupAxis(_CurationModel):
    axis: str
    label: str


class GroupCoordinate(_CurationModel):
    axis: str
    value: str
    label: str


class GroupMember(_CurationModel):
    variable: str
    delivery_column: str | None = None
    value: str | None = None
    label: str | None = None
    coords: list[GroupCoordinate] | None = None


class RegisterGroupEntry(_CurationModel):
    register_fqid: str = Field(validation_alias="register")
    key: str
    label: str
    axis: str | None = None
    axes: list[GroupAxis] | None = None
    members: list[GroupMember]

    _names = field_validator("key", "label")(_require_trimmed)


class CodeLabelPairEntry(_CurationModel):
    code: str
    label: str


class PeriodFamilyEntry(_CurationModel):
    register_fqid: str = Field(validation_alias="register")
    family_stem: str
    label: str
    slug: str

    @field_validator("slug")
    @classmethod
    def _valid_slug(cls, value: str) -> str:
        validate_slug(value, "variable")
        return value


class AliasWindowEntry(_CurationModel):
    variable: str
    variant: str
    column: str
    source_editions: list[str]
    evidence: str
    noted: str


class IdentityPartitionEntry(_CurationModel):
    variable: str
    columns: dict[str, str]
    unassigned_columns: list[str] = Field(default_factory=list)
    columns_ref: str

    @field_validator("variable")
    @classmethod
    def _family_variable(cls, value: str) -> str:
        from .fqid_slugs import _parse_variable_id

        try:
            _parse_variable_id(value)
        except RegMetaError as exc:
            raise ValueError(exc.message) from exc
        if len(value.split(".")) != 2:
            raise ValueError("must be a canonical two-part native variable key")
        return value

    @field_validator("columns")
    @classmethod
    def _columns(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("must contain at least one owned literal")
        for literal, owner in value.items():
            if not literal or literal != literal.strip():
                raise ValueError(f"literal {literal!r} must be non-empty and trimmed")
            if not owner or owner != owner.strip():
                raise ValueError(f"owner {owner!r} must be non-empty and trimmed")
        return value

    _columns_ref = field_validator("columns_ref")(_require_trimmed)

    @field_validator("unassigned_columns")
    @classmethod
    def _unassigned_columns(cls, value: list[str]) -> list[str]:
        for literal in value:
            if not literal or literal != literal.strip():
                raise ValueError(
                    f"unassigned literal {literal!r} must be non-empty and trimmed"
                )
        return value

    @model_validator(mode="after")
    def _disjoint_columns(self) -> IdentityPartitionEntry:
        from .fqid_slugs import _parse_variable_id

        family = self.variable
        for owner in self.columns.values():
            try:
                _parse_variable_id(owner)
            except RegMetaError as exc:
                raise ValueError(exc.message) from exc
            parts = owner.split(".")
            if len(parts) != 3 or ".".join(parts[:2]) != family:
                raise ValueError(
                    f"owner {owner!r} must be a canonical split key in family {family!r}"
                )
            try:
                validate_slug(parts[2], "variable")
            except ValueError as exc:
                raise ValueError(
                    f"owner {owner!r} has a non-canonical split key"
                ) from exc
        overlap = sorted(set(self.columns) & set(self.unassigned_columns))
        if overlap:
            raise ValueError(f"columns and unassigned_columns overlap: {overlap}")
        if len(set(self.unassigned_columns)) != len(self.unassigned_columns):
            raise ValueError("unassigned_columns contains duplicate literals")
        return self


class IdentityColumnOwnerEntry(_CurationModel):
    variable: str
    variant: str
    column: str
    owner: str
    ref: str

    _trimmed = field_validator("variable", "variant", "column", "owner", "ref")(
        _require_trimmed
    )

    @model_validator(mode="after")
    def _same_family(self) -> IdentityColumnOwnerEntry:
        from .fqid_slugs import _parse_variable_id

        try:
            _parse_variable_id(self.variable)
            _parse_variable_id(self.owner)
        except RegMetaError as exc:
            raise ValueError(exc.message) from exc
        owner_parts = self.owner.split(".")
        if (
            len(self.variable.split(".")) != 2
            or len(owner_parts) != 3
            or ".".join(owner_parts[:2]) != self.variable
        ):
            raise ValueError("owner must be a canonical split key in variable family")
        try:
            validate_slug(owner_parts[2], "variable")
        except ValueError as exc:
            raise ValueError(
                "owner must be a canonical split key in variable family"
            ) from exc
        return self


class IdentityRouteEntry(_CurationModel):
    deldatamangd: str
    variants: list[str]

    _trimmed = field_validator("deldatamangd")(_require_trimmed)

    @field_validator("variants")
    @classmethod
    def _variants(cls, value: list[str]) -> list[str]:
        return [_require_trimmed(item) for item in value]


class IdentityEditionSplitEntry(_CurationModel):
    variant: str
    split: str
    editions: list[str]
    source_editions: list[str]
    evidence: str
    noted: str

    _text = field_validator("evidence")(_require_trimmed)

    @model_validator(mode="after")
    def _valid(self) -> IdentityEditionSplitEntry:
        from .fqid_slugs import _parse_variant_id

        try:
            variant = _parse_variant_id(self.variant)
            split = _parse_variant_id(self.split)
        except RegMetaError as exc:
            raise ValueError(exc.message) from exc
        if len(variant) != 2 or len(split) != 3 or split[:2] != variant:
            raise ValueError("split must be a canonical three-part key under variant")
        if not self.editions or len(self.editions) != len(set(self.editions)):
            raise ValueError("editions must be non-empty and unique")
        if len(self.source_editions) != len(set(self.source_editions)):
            raise ValueError("source_editions must be unique")
        if set(self.editions) & set(self.source_editions):
            raise ValueError("editions and source_editions must be disjoint")
        for edition in (*self.editions, *self.source_editions):
            _require_trimmed(edition)
        try:
            parsed = date.fromisoformat(self.noted)
        except ValueError as exc:
            raise ValueError("noted must be an ISO date") from exc
        if parsed.isoformat() != self.noted:
            raise ValueError("noted must be a canonical ISO date")
        return self


class IdentitySplitPart(_CurationModel):
    data_type: str | None = None
    deldatamangd: str | None = None
    owner: str

    _trimmed = field_validator("data_type", "deldatamangd")(_require_trimmed)

    @model_validator(mode="after")
    def _one_discriminator(self) -> IdentitySplitPart:
        if (self.data_type is None) == (self.deldatamangd is None):
            raise ValueError(
                "split part needs exactly one of data_type or deldatamangd"
            )
        return self

    @field_validator("owner")
    @classmethod
    def _owner(cls, value: str) -> str:
        from .fqid_slugs import _parse_variable_id

        try:
            _parse_variable_id(value)
        except RegMetaError as exc:
            raise ValueError(exc.message) from exc
        if len(value.split(".")) != 3:
            raise ValueError("must be a canonical three-part split key")
        try:
            validate_slug(value.split(".")[2], "variable")
        except ValueError as exc:
            raise ValueError("must be a canonical three-part split key") from exc
        return value


class IdentitySplitEntry(_CurationModel):
    variable: str
    by: Literal["data_type", "deldatamangd"]
    parts: list[IdentitySplitPart]

    _variable = field_validator("variable")(_require_trimmed)

    @model_validator(mode="after")
    def _matching_discriminator(self) -> IdentitySplitEntry:
        if any(getattr(part, self.by) is None for part in self.parts):
            raise ValueError(f"split parts must use {self.by} when by = {self.by!r}")
        return self


class IdentityRenameEntry(_CurationModel):
    deldatamangd: str
    variable: str
    name: str
    column: str

    _trimmed = field_validator("deldatamangd", "variable", "name", "column")(
        _require_trimmed
    )


class IdentityCuration(_CurationModel):
    partition: list[IdentityPartitionEntry] = Field(default_factory=list)
    column_owner: list[IdentityColumnOwnerEntry] = Field(default_factory=list)
    route: list[IdentityRouteEntry] = Field(default_factory=list)
    edition_split: list[IdentityEditionSplitEntry] = Field(default_factory=list)
    split: list[IdentitySplitEntry] = Field(default_factory=list)
    rename: list[IdentityRenameEntry] = Field(default_factory=list)


class RepresentationCuration(_CurationModel):
    period_family: list[PeriodFamilyEntry] = Field(default_factory=list)
    alias_window: list[AliasWindowEntry] = Field(default_factory=list)


class CodingEntry(_CurationModel):
    variable: str
    variant: str
    column: str
    periods: list[list[str]]
    reason: str
    source: str

    _trimmed = field_validator("variant", "column", "reason", "source")(
        _require_trimmed
    )

    @field_validator("variable")
    @classmethod
    def _variable_id(cls, value: str) -> str:
        from .fqid_slugs import _parse_variable_id

        try:
            _parse_variable_id(value)
        except RegMetaError as exc:
            raise ValueError(exc.message) from exc
        return value

    @field_validator("periods")
    @classmethod
    def _periods(cls, value: list[list[str]]) -> list[list[str]]:
        if not value:
            raise ValueError("periods must contain at least one finite window")
        for window in value:
            _coding_window(window)
        return value


def _coding_window(window: list[str]) -> None:
    if len(window) != 2:
        raise ValueError("coding window must be [from, to]")
    for bound in window:
        if len(bound) != 10 or bound[4] != "-" or bound[7] != "-":
            raise ValueError("coding window bounds must be ISO yyyy-mm-dd dates")
        try:
            date.fromisoformat(bound)
        except ValueError as exc:
            raise ValueError("coding window bounds must be valid ISO dates") from exc
    if window[0] > window[1]:
        raise ValueError("coding window bounds are reversed")


def _coding_members(value: list[list[str]]) -> list[list[str]]:
    if not value or any(len(pair) != 2 for pair in value):
        raise ValueError("members must be nonempty [code, label] pairs")
    if any(not code or not label for code, label in value):
        raise ValueError("member codes and labels must be nonempty")
    if len({tuple(pair) for pair in value}) != len(value):
        raise ValueError("members must be unique")
    return value


class CodingChoiceEntry(CodingEntry):
    keep: str
    keep_members: list[list[str]] | None = None
    over: list[str]

    _keep = field_validator("keep")(_require_trimmed)

    @field_validator("keep_members")
    @classmethod
    def _members(cls, value: list[list[str]] | None) -> list[list[str]] | None:
        return None if value is None else _coding_members(value)

    @field_validator("over")
    @classmethod
    def _over(cls, value: list[str]) -> list[str]:
        trimmed = [_require_trimmed(label) for label in value]
        if not trimmed or len(set(trimmed)) != len(trimmed):
            raise ValueError("over must name distinct competing labels")
        return trimmed


class CodingExtendEntry(CodingEntry):
    list: str
    list_members: list[list[str]] | None = None
    witness: list[str]

    _list = field_validator("list")(_require_trimmed)

    @field_validator("list_members")
    @classmethod
    def _members(cls, value: list[list[str]] | None) -> list[list[str]] | None:
        return None if value is None else _coding_members(value)

    @field_validator("witness")
    @classmethod
    def _witness(cls, value: list[str]) -> list[str]:
        _coding_window(value)
        return value


class CodingCuration(_CurationModel):
    choice: list[CodingChoiceEntry] = Field(default_factory=list)
    uncoded: list[CodingEntry] = Field(default_factory=list)
    omit: list[CodingEntry] = Field(default_factory=list)
    extend: list[CodingExtendEntry] = Field(default_factory=list)


class AcknowledgeEntry(_CurationModel):
    code: str
    subject: str
    refs: list[str]
    fields: list[str] = Field(default_factory=list)
    valid_from: str | None = None
    valid_to: str | None = None
    reason: str
    evidence: str

    _trimmed = field_validator("code", "subject", "reason", "evidence")(
        _require_trimmed
    )

    @field_validator("refs")
    @classmethod
    def _refs(cls, value: list[str]) -> list[str]:
        return [_require_trimmed(item) for item in value]

    @field_validator("fields")
    @classmethod
    def _fields(cls, value: list[str]) -> list[str]:
        fields = [_require_trimmed(item) for item in value]
        if len(fields) != len(set(fields)):
            raise ValueError("acknowledged fields must be unique")
        return fields

    @model_validator(mode="after")
    def _period(self) -> AcknowledgeEntry:
        if (self.valid_from is None) != (self.valid_to is None):
            raise ValueError("valid_from and valid_to must both be set or both omitted")
        for field in ("valid_from", "valid_to"):
            value = getattr(self, field)
            if value is None:
                continue
            try:
                parsed = date.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(f"{field} must be a canonical ISO date") from exc
            if parsed.isoformat() != value:
                raise ValueError(f"{field} must be a canonical ISO date")
        if (
            self.valid_from is not None
            and self.valid_to is not None
            and self.valid_to < self.valid_from
        ):
            raise ValueError("valid_to must not precede valid_from")
        return self


class RegisterCuration(_CurationModel):
    """One register file with a closed, typed table set."""

    register_info: RegisterIdentity = Field(validation_alias="register")
    variant: list[RegisterVariantSlug] = Field(default_factory=list)
    variable: list[RegisterVariableSlug] = Field(default_factory=list)
    errata: ErrataCuration = Field(default_factory=ErrataCuration)
    enrichment: EnrichmentCuration = Field(default_factory=EnrichmentCuration)
    group: list[RegisterGroupEntry] = Field(default_factory=list)
    code_label_pair: list[CodeLabelPairEntry] = Field(default_factory=list)
    representation: RepresentationCuration = Field(
        default_factory=RepresentationCuration
    )
    identity: IdentityCuration = Field(default_factory=IdentityCuration)
    coding: CodingCuration = Field(default_factory=CodingCuration)
    acknowledge: list[AcknowledgeEntry] = Field(default_factory=list)

    _source_file: str = PrivateAttr(default="")

    @property
    def source_file(self) -> str:
        return self._source_file


class ClassificationGroupMemberEntry(_CurationModel):
    classification: str
    value: str
    label: str

    _member_fields = field_validator("classification", "value", "label")(
        _require_trimmed
    )


class ClassificationGroupEntry(_CurationModel):
    key: str
    label: str
    axis: str | None = None
    members: list[ClassificationGroupMemberEntry]

    _names = field_validator("key", "label")(_require_trimmed)

    @field_validator("axis")
    @classmethod
    def _axis(cls, value: str | None) -> str | None:
        return None if value is None else _require_trimmed(value)


class ClassificationGroups(_CurationModel):
    classification_group: list[ClassificationGroupEntry] = Field(default_factory=list)


@dataclass(frozen=True)
class CurationTree:
    """The classification, register, and global curation files."""

    classifications: tuple[CuratedClassification, ...]
    classification_families: ClassificationFamilies
    registers: tuple[RegisterCuration, ...]
    classification_groups: ClassificationGroups
    relations: CuratedRelations
    tags: tuple[CuratedTag, ...]
    lineage: LineageConfig
    root: Path


def _require_repo_curation_dir() -> Path:
    root = repo_curation_dir()
    if root is None:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="curation_tree_not_found",
            error_class="configuration",
            message="reg_meta_build/curation/ not found; cannot read curation.",
            remediation="Run from the maintainer repo checkout.",
        )
    return root


def _load_classification(path: Path, file: str) -> CuratedClassification:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise curation_error(
            _CODE, f"Could not parse {file}: {exc}", "Fix the TOML syntax."
        ) from exc
    try:
        entry = CuratedClassification.model_validate(data, context={"file": file})
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        where = ".".join(str(part) for part in error["loc"])
        raise curation_error(
            _CODE,
            f"{file}: `{where}`: {error['msg']}.",
            "Write only the documented [classification] and [binding] keys "
            "(see reg_meta_build/CLASSIFICATIONS.md).",
        ) from exc
    if entry.classification.short_name != path.stem:
        raise curation_error(
            _CODE,
            f"{file}: short_name {entry.classification.short_name!r} does not "
            "match the file name.",
            "Name each classification file <short_name>.toml.",
        )
    return entry


def load_classifications(root: Path) -> tuple[CuratedClassification, ...]:
    """Read ``root/classifications/*.toml`` in sorted path order.

    No slug, value-set label or bound variable may appear in two files.
    """
    directory = root / CLASSIFICATIONS_DIR
    if not directory.is_dir():
        raise curation_error(
            _CODE,
            f"Classification curation directory {directory} not found.",
            "Create curation/classifications/ with one <short_name>.toml per book.",
        )
    entries: list[CuratedClassification] = []
    owners: dict[tuple[str, str], str] = {}
    for path in sorted(directory.iterdir()):
        file = f"{CLASSIFICATIONS_DIR}/{path.name}"
        if path.suffix != ".toml" or not path.is_file():
            raise curation_error(
                _CODE,
                f"{file} is not a classification TOML file.",
                "Keep only <short_name>.toml files in curation/classifications/.",
            )
        entry = _load_classification(path, file)
        claims = [
            ("slug", entry.classification.slug),
            *(("label", label) for label in entry.binding.value_set_labels),
            *(("variable", bound.variable) for bound in entry.binding.variable),
        ]
        for kind, value in claims:
            if (kind, value) in owners:
                raise curation_error(
                    _CODE,
                    f"{file}: {kind} {value!r} is also declared in "
                    f"{owners[kind, value]}.",
                    f"Declare a {kind} once, in one classification file.",
                )
            owners[kind, value] = file
        entries.append(entry)
    return tuple(entries)


def load_classification_families(
    classifications: tuple[CuratedClassification, ...],
) -> ClassificationFamilies:
    """Group edition slugs and exact aliases from their validated metadata."""
    grouped: dict[str, list[ClassificationMetadata]] = {}
    for entry in classifications:
        metadata = entry.classification
        if metadata.family is not None:
            grouped.setdefault(metadata.family, []).append(metadata)
    families = []
    for key, members in sorted(grouped.items()):
        try:
            families.append(
                ClassificationFamily(
                    key=key,
                    members=tuple(member.slug for member in members),
                    aliases=tuple(
                        alias for member in members for alias in member.family_aliases
                    ),
                )
            )
        except ValidationError as exc:
            raise curation_error(
                _CODE,
                f"classifications/{members[0].short_name}.toml: "
                f"invalid family {key!r}: {exc.errors(include_url=False)[0]['msg']}.",
                "Curate at least two editions per family with unique aliases.",
            ) from exc
    return ClassificationFamilies(family=tuple(families))


def _register_arrays(
    entry: RegisterCuration,
) -> tuple[tuple[str, Sequence[BaseModel]], ...]:
    return (
        ("errata.delivered", entry.errata.delivered),
        ("errata.column", entry.errata.column),
        ("errata.version", entry.errata.version),
        ("errata.edition_period", entry.errata.edition_period),
        ("enrichment.description", entry.enrichment.description),
        ("enrichment.alias", entry.enrichment.alias),
        ("group", entry.group),
        ("code_label_pair", entry.code_label_pair),
        ("representation.period_family", entry.representation.period_family),
        ("representation.alias_window", entry.representation.alias_window),
        ("identity.partition", entry.identity.partition),
        ("identity.column_owner", entry.identity.column_owner),
        ("identity.route", entry.identity.route),
        ("identity.edition_split", entry.identity.edition_split),
        ("identity.split", entry.identity.split),
        ("identity.rename", entry.identity.rename),
        ("coding.choice", entry.coding.choice),
        ("coding.uncoded", entry.coding.uncoded),
        ("coding.omit", entry.coding.omit),
        ("coding.extend", entry.coding.extend),
        ("acknowledge", entry.acknowledge),
    )


def _register_path(directory: Path, path: Path) -> tuple[str, str]:
    relative = path.relative_to(directory).with_suffix("")
    parts = relative.parts
    if len(parts) not in (2, 3):
        raise curation_error(
            _CODE,
            f"curation/registers/{relative.as_posix()}.toml: expected "
            "<provider>/<slug>.toml or <provider>/<family>/<slug>.toml.",
            "Keep register files at one or two directories below registers/.",
        )
    return parts[0], parts[-1]


def _check_register_ref(
    value: str, *, expected: str, file: str, table: str, index: int
) -> None:
    parts = value.split("/")
    if len(parts) != 2 or not all(parts):
        raise curation_error(
            _CODE,
            f"{file} [[{table}]] entry {index}: register {value!r} must be "
            "a provider/register coordinate.",
            "Use the register file's [register] provider and slug.",
        )
    if value != expected:
        raise curation_error(
            _CODE,
            f"{file} [[{table}]] entry {index}: register {value!r} does not "
            f"match {expected!r}.",
            "Move the entry to its register file or correct its register field.",
        )


def _validate_register_scope(entry: RegisterCuration, file: str) -> None:
    from .fqid_slugs import _parse_variant_id

    identity = entry.register_info
    expected = f"{identity.provider}/{identity.slug}"
    edition_owners: set[tuple[str, str]] = set()
    split_owners: set[str] = set()
    variant_ids = {variant.native_id for variant in entry.variant}
    native_variant_slugs = {
        variant.slug
        for variant in entry.variant
        if len(_parse_variant_id(variant.native_id)) == 2
    }
    for index, split in enumerate(entry.identity.edition_split, 1):
        where = f"{file} [[identity.edition_split]] entry {index}"
        if identity.provider != "scb" or not split.variant.startswith(
            f"{identity.native_id}."
        ):
            raise curation_error(
                _CODE,
                f"{where}: edition split must belong to this SCB register.",
                "Use the owning SCB register and native variant.",
            )
        if split.split not in variant_ids:
            raise curation_error(
                _CODE,
                f"{where}: split {split.split!r} has no [[variant]] entry.",
                "Declare the split variant and its slug.",
            )
        pairs = {(split.variant, edition) for edition in split.editions}
        repeated = edition_owners.intersection(pairs)
        if repeated:
            raise curation_error(
                _CODE,
                f"{where}: editions assigned twice: {sorted(repeated)!r}.",
                "Assign each edition name once.",
            )
        if split.variant in split_owners:
            raise curation_error(
                _CODE,
                f"{where}: native variant {split.variant!r} has multiple edition splits.",
                "Declare one complete edition inventory for the native variant.",
            )
        edition_owners.update(pairs)
        split_owners.add(split.variant)
    if identity.provider in {"scb", "sos"} and (
        identity.native_id is None or not identity.native_id.isdecimal()
    ):
        raise curation_error(
            _CODE,
            f"{file} [register]: native_id must be a decimal string for "
            f"{identity.provider!r}, got {identity.native_id!r}.",
            "Set the register's source native id in this file.",
        )

    for table, rows in _register_arrays(entry):
        for index, row in enumerate(rows, start=1):
            if isinstance(row, ErrataEditionPeriodEntry) and (
                identity.provider != "scb" or row.variant not in native_variant_slugs
            ):
                raise curation_error(
                    _CODE,
                    f"{file} [[{table}]] entry {index}: variant {row.variant!r} "
                    "must name a native variant of an SCB register.",
                    "Use a native [[variant]] slug in the owning SCB register file.",
                )
            register_refs: list[str] = []
            if isinstance(
                row,
                (
                    EnrichmentDescriptionEntry,
                    EnrichmentAliasEntry,
                    RegisterGroupEntry,
                    PeriodFamilyEntry,
                ),
            ):
                register_refs.append(row.register_fqid)
            elif isinstance(row, CodeLabelPairEntry):
                register_refs.extend(
                    (row.code.rsplit("/", 1)[0], row.label.rsplit("/", 1)[0])
                )
            elif isinstance(row, AliasWindowEntry):
                register_refs.append(row.variable.rsplit("/", 1)[0])
            for value in register_refs:
                _check_register_ref(
                    value, expected=expected, file=file, table=table, index=index
                )
            if isinstance(
                row, (IdentityPartitionEntry, IdentityColumnOwnerEntry, CodingEntry)
            ):
                native_id = identity.native_id
                if native_id is not None and not row.variable.startswith(
                    native_id + "."
                ):
                    raise curation_error(
                        _CODE,
                        f"{file} [[{table}]] entry {index}: variable "
                        f"{row.variable!r} does not belong to native_id {native_id!r}.",
                        "Move the entry to the register file owning that native family.",
                    )
            if isinstance(row, IdentitySplitEntry):
                native_id = identity.native_id
                if native_id is not None:
                    for part in row.parts:
                        if not part.owner.startswith(native_id + "."):
                            raise curation_error(
                                _CODE,
                                f"{file} [[{table}]] entry {index}: split owner "
                                f"{part.owner!r} does not belong to native_id "
                                f"{native_id!r}.",
                                "Move the split to the register file owning its native family.",
                            )
            if (
                isinstance(row, (IdentityRouteEntry, IdentityRenameEntry))
                and identity.provider != "sos"
            ):
                raise curation_error(
                    _CODE,
                    f"{file} [[{table}]] entry {index}: identity routing and "
                    f"renaming applies to SOS registers, not {identity.provider!r}.",
                    "Move the entry to the SOS register file it describes.",
                )


def _load_register_file(path: Path, directory: Path) -> RegisterCuration:
    relative = path.relative_to(directory.parent).as_posix()
    file = f"curation/{relative}"
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise curation_error(
            _CODE, f"Could not parse {file}: {exc}", "Fix the register TOML syntax."
        ) from exc
    try:
        entry = RegisterCuration.model_validate(raw)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        parts = error["loc"]
        table = ".".join(str(part) for part in parts if not isinstance(part, int))
        index = next((part + 1 for part in parts if isinstance(part, int)), None)
        where = f"[[{table}]] entry {index}" if index is not None else table
        raise curation_error(
            _CODE,
            f"{file} {where}: {error['msg']}.",
            "Use only the documented strict register tables and fields.",
        ) from exc

    provider, slug = _register_path(directory, path)
    if (entry.register_info.provider, entry.register_info.slug) != (provider, slug):
        raise curation_error(
            _CODE,
            f"{file} [register] ({entry.register_info.provider}/"
            f"{entry.register_info.slug}) does not match its path ({provider}/{slug}).",
            "Match [register].provider and [register].slug to the file path.",
        )
    entry._source_file = file
    _validate_register_scope(entry, file)
    for table, rows in _register_arrays(entry):
        seen: set[str] = set()
        edition_period_keys: set[tuple[str, str]] = set()
        for index, row in enumerate(rows, start=1):
            if isinstance(row, ErrataEditionPeriodEntry):
                coordinate = (row.variant, row.name)
                if coordinate in edition_period_keys:
                    raise curation_error(
                        _CODE,
                        f"{file} [[{table}]] entry {index}: duplicate edition period "
                        f"for {coordinate!r}.",
                        "Keep one period per variant and edition name.",
                    )
                edition_period_keys.add(coordinate)
            key = json.dumps(
                row.model_dump(mode="json"), ensure_ascii=False, sort_keys=True
            )
            if key in seen:
                raise curation_error(
                    _CODE,
                    f"{file} [[{table}]] entry {index}: duplicate entry.",
                    "Keep one declaration for each entry.",
                )
            seen.add(key)
    return entry


def load_register_files(root: Path) -> tuple[RegisterCuration, ...]:
    """Read register TOMLs by sorted path and reject duplicate register IDs."""
    directory = root / "registers"
    if not directory.is_dir():
        return ()
    entries: list[RegisterCuration] = []
    owners: dict[tuple[str, str], str] = {}
    native_ids: dict[tuple[str, str], str] = {}
    label_owners: dict[tuple[str, str], tuple[str, str]] = {}
    for path in sorted(directory.rglob("*.toml")):
        if path.name.endswith(".auto.toml"):
            continue
        if not path.is_file():
            raise curation_error(
                _CODE,
                f"curation/{path.relative_to(root).as_posix()} is not a file.",
                "Keep only register TOML files under curation/registers/.",
            )
        entry = _load_register_file(path, directory)
        rel = path.relative_to(root).as_posix()
        identity = entry.register_info
        key = (identity.provider, identity.slug)
        if key in owners:
            raise curation_error(
                _CODE,
                f"curation/{rel} [register]: duplicate register {key[0]}/{key[1]} "
                f"also declared in curation/{owners[key]}.",
                "Keep one file per provider/register.",
            )
        owners[key] = rel
        for label in identity.source_labels:
            label_key = identity.provider, label.casefold()
            if (prior := label_owners.get(label_key)) and prior[0] != identity.slug:
                raise curation_error(
                    _CODE,
                    f"curation/{rel} [register].source_labels: duplicate source "
                    f"label {label!r} for {identity.provider}/{identity.slug}; "
                    f"also declared by {identity.provider}/{prior[0]} in "
                    f"curation/{prior[1]}.",
                    "Keep each provider's source label on one register.",
                )
            label_owners[label_key] = identity.slug, rel
        if identity.native_id is not None:
            native_key = (identity.provider, identity.native_id)
            if native_key in native_ids:
                raise curation_error(
                    _CODE,
                    f"curation/{rel} [register]: duplicate native_id "
                    f"{identity.native_id!r} also declared in "
                    f"curation/{native_ids[native_key]}.",
                    "Keep each provider/native_id in one register file.",
                )
            native_ids[native_key] = rel
        entries.append(entry)
    return tuple(entries)


def load_classification_groups(root: Path) -> ClassificationGroups:
    """Read ``classification_groups.toml`` with a closed strict model."""
    path = root / "classification_groups.toml"
    if not path.is_file():
        return ClassificationGroups()
    file = "curation/classification_groups.toml"
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        groups = ClassificationGroups.model_validate(raw)
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise curation_error(
            _CODE, f"Could not parse {file}: {exc}", "Fix the TOML syntax."
        ) from exc
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = error["loc"]
        index = next((part + 1 for part in location if isinstance(part, int)), None)
        table = ".".join(str(part) for part in location if not isinstance(part, int))
        where = f"[[{table}]] entry {index}" if index is not None else f"[[{table}]]"
        raise curation_error(
            _CODE,
            f"{file} {where}: {error['msg']}.",
            "Use only the documented classification-group tables and fields.",
        ) from exc
    seen: set[str] = set()
    for index, group in enumerate(groups.classification_group, start=1):
        if group.key in seen:
            raise curation_error(
                _CODE,
                f"{file} [[classification_group]] entry {index}: duplicate key {group.key!r}.",
                "Keep each classification-group key once.",
            )
        seen.add(group.key)
    return groups


def load_curation_tree(root: Path) -> CurationTree:
    """Read the classification files and the global files under ``root``."""
    lineage = load_lineage_config(root / "lineage.toml")
    if lineage.overrides:
        raise curation_error(
            "lineage_invalid",
            'lineage.toml: per-variable [lineage."…"] overrides have no consumer.',
            'Remove the [lineage."…"] tables; keep [lineage_defaults] only.',
        )
    classifications = load_classifications(root)
    return CurationTree(
        classifications=classifications,
        classification_families=load_classification_families(classifications),
        registers=load_register_files(root),
        classification_groups=load_classification_groups(root),
        relations=load_relations(root / "relations.toml"),
        tags=load_tags(root / "tags.toml"),
        lineage=lineage,
        root=root,
    )


def declared_short_names() -> frozenset[str]:
    """The checkout's classification short names, for reference validation."""
    return frozenset(
        entry.classification.short_name
        for entry in load_classifications(_require_repo_curation_dir())
    )
