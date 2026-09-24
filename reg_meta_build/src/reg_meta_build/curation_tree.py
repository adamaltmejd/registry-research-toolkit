"""Read the tracked curation tree under ``reg_meta_build/curation/``.

``classifications/<short_name>.toml`` holds one classification: its metadata and
sentinels in ``[classification]``, and in ``[binding]`` the value-set labels behind
the label binding rule plus the curated ``[[binding.variable]]`` bindings.
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
    _is_split_base_pair,
    iter_curated_provider_entries,
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


class RegisterIdentity(_CurationModel):
    """The coordinate asserted by a register file's path."""

    provider: str
    slug: str
    native_id: str | None = None
    name: str | None = None

    _provider = field_validator("provider")(_require_trimmed)
    _slug = field_validator("slug")(_require_trimmed)
    _native_id = field_validator("native_id")(_require_trimmed)
    _name = field_validator("name")(_require_trimmed)

    @field_validator("slug")
    @classmethod
    def _valid_slug(cls, value: str) -> str:
        validate_slug(value, "register")
        return value


class ErrataVersionEntry(_CurationModel):
    variant: str
    name: str
    evidence: str
    noted: str


class ErrataDeliveredEntry(_CurationModel):
    variant: str
    column: str
    versions: list[str]
    evidence: str
    noted: str
    upstream: str | None = None


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
    slug: str | None = None


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
        for literal, owner in self.columns.items():
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
                raise ValueError(f"owner {owner!r} has a non-canonical split key") from exc
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
            raise ValueError("owner must be a canonical split key in variable family") from exc
        return self


class IdentityRouteEntry(_CurationModel):
    deldatamangd: str
    variants: list[str]

    _trimmed = field_validator("deldatamangd")(_require_trimmed)

    @field_validator("variants")
    @classmethod
    def _variants(cls, value: list[str]) -> list[str]:
        return [_require_trimmed(item) for item in value]


class IdentitySplitPart(_CurationModel):
    data_type: str
    owner: str

    _trimmed = field_validator("data_type")(_require_trimmed)

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
    by: Literal["data_type"]
    parts: list[IdentitySplitPart]

    _variable = field_validator("variable")(_require_trimmed)


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
    split: list[IdentitySplitEntry] = Field(default_factory=list)
    rename: list[IdentityRenameEntry] = Field(default_factory=list)


class RepresentationCuration(_CurationModel):
    period_family: list[PeriodFamilyEntry] = Field(default_factory=list)
    alias_window: list[AliasWindowEntry] = Field(default_factory=list)


class AcknowledgeEntry(_CurationModel):
    code: str
    subject: str
    refs: list[str]
    reason: str
    evidence: str

    _trimmed = field_validator("code", "subject", "reason", "evidence")(
        _require_trimmed
    )

    @field_validator("refs")
    @classmethod
    def _refs(cls, value: list[str]) -> list[str]:
        return [_require_trimmed(item) for item in value]


class RegisterCuration(_CurationModel):
    """One register file with a closed, typed table set."""

    register_info: RegisterIdentity = Field(validation_alias="register")
    errata: ErrataCuration = Field(default_factory=ErrataCuration)
    enrichment: EnrichmentCuration = Field(default_factory=EnrichmentCuration)
    group: list[RegisterGroupEntry] = Field(default_factory=list)
    code_label_pair: list[CodeLabelPairEntry] = Field(default_factory=list)
    representation: RepresentationCuration = Field(
        default_factory=RepresentationCuration
    )
    identity: IdentityCuration = Field(default_factory=IdentityCuration)
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
    registers: tuple[RegisterCuration, ...]
    classification_groups: ClassificationGroups
    relations: CuratedRelations
    tags: tuple[CuratedTag, ...]
    lineage: LineageConfig


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


def _register_arrays(
    entry: RegisterCuration,
) -> tuple[tuple[str, Sequence[BaseModel]], ...]:
    return (
        ("errata.delivered", entry.errata.delivered),
        ("errata.column", entry.errata.column),
        ("errata.version", entry.errata.version),
        ("enrichment.description", entry.enrichment.description),
        ("enrichment.alias", entry.enrichment.alias),
        ("group", entry.group),
        ("code_label_pair", entry.code_label_pair),
        ("representation.period_family", entry.representation.period_family),
        ("representation.alias_window", entry.representation.alias_window),
        ("identity.partition", entry.identity.partition),
        ("identity.column_owner", entry.identity.column_owner),
        ("identity.route", entry.identity.route),
        ("identity.split", entry.identity.split),
        ("identity.rename", entry.identity.rename),
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
    identity = entry.register_info
    expected = f"{identity.provider}/{identity.slug}"
    if identity.provider in {"scb", "sos"} and (
        identity.native_id is None or not identity.native_id.isdecimal()
    ):
        raise curation_error(
            _CODE,
            f"{file} [register]: native_id must be a decimal string for "
            f"{identity.provider!r}, got {identity.native_id!r}.",
            "Copy the source native id from fqid_slugs/<provider>.toml.",
        )

    for table, rows in _register_arrays(entry):
        for index, row in enumerate(rows, start=1):
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
            if isinstance(row, (IdentityPartitionEntry, IdentityColumnOwnerEntry)):
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
            if isinstance(row, (IdentityRouteEntry, IdentityRenameEntry)):
                if identity.provider != "sos":
                    raise curation_error(
                        _CODE,
                        f"{file} [[{table}]] entry {index}: identity routing and "
                        f"renaming applies to SOS registers, not {identity.provider!r}.",
                        "Move the entry to the SOS register file it describes.",
                    )
            if isinstance(row, AcknowledgeEntry):
                subject = row.subject.split("/")
                if len(subject) < 3 or "/".join(subject[:2]) != expected:
                    raise curation_error(
                        _CODE,
                        f"{file} [[{table}]] entry {index}: acknowledge subject "
                        f"{row.subject!r} does not belong to {expected!r}.",
                        "Scope the acknowledgement subject to this register FQID.",
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
        for index, row in enumerate(rows, start=1):
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
    for path in sorted(directory.rglob("*.toml")):
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
    slug_dir = root.parent / "fqid_slugs"
    if slug_dir.is_dir():
        source_ids: dict[tuple[str, str], list[str]] = {}
        slug_entries_by_provider: dict[str, list] = {}
        for slug_entry in iter_curated_provider_entries(slug_dir):
            provider = slug_entry.provider
            if provider is None:
                continue
            slug_entries_by_provider.setdefault(provider, []).append(slug_entry)
            if slug_entry.kind == "register" and slug_entry.slug is not None:
                source_ids.setdefault((provider, slug_entry.slug), []).append(
                    slug_entry.source_id
                )
        for entry in entries:
            identity = entry.register_info
            if identity.provider not in {"scb", "sos"}:
                continue
            file = entry.source_file
            matching = source_ids.get((identity.provider, identity.slug), [])
            if len(matching) != 1 or matching[0] != identity.native_id:
                raise curation_error(
                    _CODE,
                    f"{file} [register]: native_id {identity.native_id!r} does not "
                    f"match fqid_slugs/{identity.provider}.toml for "
                    f"{identity.provider}/{identity.slug} ({matching}).",
                    "Copy the register's source id from its [register] slug entry.",
                )
        register_by_native_id = {
            (item.register_info.provider, item.register_info.native_id): item
            for item in entries
            if item.register_info.native_id is not None
        }
        for provider, slug_entries in sorted(slug_entries_by_provider.items()):
            slug_holders: dict[tuple[str, str], list] = {}
            for slug_entry in slug_entries:
                if slug_entry.kind != "variable" or slug_entry.slug is None:
                    continue
                native_id = slug_entry.source_id.split(".", 1)[0]
                slug_holders.setdefault((native_id, slug_entry.slug), []).append(
                    slug_entry
                )
            for (native_id, slug), holders in slug_holders.items():
                if len(holders) < 2:
                    continue
                pair = len(holders) == 2 and _is_split_base_pair(*holders)
                register = register_by_native_id.get((provider, native_id))
                family = next(
                    (item for item in holders if len(item.source_id.split(".")) == 2),
                    None,
                )
                if (
                    pair
                    and family is not None
                    and register is not None
                    and any(
                        partition.variable == family.source_id
                        for partition in register.identity.partition
                    )
                ):
                    continue
                raise curation_error(
                    "slug_toml_invalid",
                    f"fqid_slugs/{provider}.toml: slug {slug!r} reused by "
                    f"split/base entries in register {native_id!r} without a "
                    f"matching [[identity.partition]] in "
                    f"{register.source_file if register is not None else f'curation/registers/{provider}/{slug}.toml'}.",
                    "Keep split/base slug reuse only when the register file declares ownership for that native family.",
                )
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
    return CurationTree(
        classifications=load_classifications(root),
        registers=load_register_files(root),
        classification_groups=load_classification_groups(root),
        relations=load_relations(root / "relations.toml"),
        tags=load_tags(root / "tags.toml"),
        lineage=lineage,
    )


def declared_short_names() -> frozenset[str]:
    """The checkout's classification short names, for reference validation."""
    return frozenset(
        entry.classification.short_name
        for entry in load_classifications(_require_repo_curation_dir())
    )
