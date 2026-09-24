"""Read the tracked curation tree under ``reg_meta_build/curation/``.

``classifications/<short_name>.toml`` holds one classification: its metadata and
sentinels in ``[classification]``, and in ``[binding]`` the value-set labels behind
the label binding rule plus the curated ``[[binding.variable]]`` bindings.
``relations.toml``, ``tags.toml`` and ``lineage.toml`` stay at the root as global
files. The reader returns typed, validated entries and never touches prepared
data. Files enumerate in sorted path order, and every error names its file.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
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
from .fqid_slugs import load_lineage_config
from .normalization import normalize_text
from .relations import load_relations
from .resolved_metadata import _variable
from .tags import load_tags

if TYPE_CHECKING:
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


@dataclass(frozen=True)
class CurationTree:
    """The classification files and the global curation files."""

    classifications: tuple[CuratedClassification, ...]
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
