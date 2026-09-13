"""The documented CIS 2014–2016 cooperation-matrix answer partition.

SCB's machine export gives all 54 named answer columns one VarId and one CVID.
The accompanying quality declaration distinguishes the answers.  This module
loads that one reviewed evidence declaration; the SCB adapter applies it at the
source-instance boundary before generic coalescing.

This is intentionally not a matrix-discovery or source-mapping framework.
Other waves need their own reviewed meaning evidence before they can acquire
answer identities or continuity links.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)
from reg_meta.fqid import derive_variable_slug

from ._curation import curation_error

if TYPE_CHECKING:
    from pathlib import Path


_FILE_NAME = "curation/cis2016-matrix-meaning-evidence.json"
_CODE = "cis2016_matrix_invalid"

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
StableKey = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
    ),
]
AxisKey = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        pattern=r"^[a-z0-9]+(?:_[a-z0-9]+)*$",
    ),
]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class _CurationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MatrixSelector(_CurationModel):
    register_fqid: NonEmpty = Field(alias="register")
    register_id: Annotated[int, Field(ge=0)]
    variant: NonEmpty
    register_variant_id: Annotated[int, Field(ge=0)]
    edition: NonEmpty
    regver_id: Annotated[int, Field(ge=0)]
    var_id: Annotated[int, Field(ge=0)]
    cvid: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def _scb_register_fqid(self) -> MatrixSelector:
        parts = self.register_fqid.split("/")
        if len(parts) != 2 or parts[0] != "scb" or not parts[1]:
            raise ValueError("selector.register must be a 2-segment scb/* FQID")
        return self


class MatrixEvidence(_CurationModel):
    document: NonEmpty
    url: NonEmpty
    sha256: Sha256
    question: NonEmpty
    noted: Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$")]


class MatrixAxis(_CurationModel):
    key: AxisKey
    label_en: NonEmpty


class MatrixAnswer(_CurationModel):
    key: StableKey
    slug: StableKey
    columns: tuple[NonEmpty, ...]
    label_en: NonEmpty
    definition_en: NonEmpty
    partner: MatrixAxis
    response: MatrixAxis
    source_pages: dict[NonEmpty, Annotated[int, Field(ge=1)]]
    meaning_evidence: NonEmpty | None = None

    @model_validator(mode="after")
    def _complete_answer_evidence(self) -> MatrixAnswer:
        if not self.columns:
            raise ValueError("an answer needs at least one source column")
        if len(set(self.columns)) != len(self.columns):
            raise ValueError(f"answer {self.key!r} repeats a source column")
        if set(self.source_pages) != set(self.columns):
            raise ValueError(
                f"answer {self.key!r} source_pages must name exactly its columns"
            )
        if len(self.columns) > 1 and self.meaning_evidence is None:
            raise ValueError(
                f"answer {self.key!r} joins aliases without meaning_evidence"
            )
        if derive_variable_slug(self.slug) != self.slug:
            raise ValueError(f"answer {self.key!r} has an invalid variable slug")
        return self


class Cis2016Matrix(_CurationModel):
    selector: MatrixSelector
    evidence: MatrixEvidence
    question_label: NonEmpty
    historical_variable_slug: StableKey
    axes: tuple[MatrixAxis, MatrixAxis]
    answers: tuple[MatrixAnswer, ...]

    @model_validator(mode="after")
    def _complete_partition(self) -> Cis2016Matrix:
        if derive_variable_slug(self.historical_variable_slug) != (
            self.historical_variable_slug
        ):
            raise ValueError("historical_variable_slug is not a valid variable slug")
        if tuple(axis.key for axis in self.axes) != ("partner", "response"):
            raise ValueError("axes must be ordered as partner, response")
        if len(self.answers) < 2:
            raise ValueError("the matrix partition needs at least two answers")

        for attr in ("key", "slug"):
            values = [getattr(answer, attr) for answer in self.answers]
            if len(set(values)) != len(values):
                raise ValueError(f"matrix answers repeat {attr} selectors")
        if self.historical_variable_slug in {answer.slug for answer in self.answers}:
            raise ValueError(
                "historical_variable_slug conflicts with a matrix answer slug"
            )

        columns = [column for answer in self.answers for column in answer.columns]
        if len(set(columns)) != len(columns):
            raise ValueError("matrix answers assign a source column more than once")

        coordinates = [
            (answer.partner.key, answer.response.key) for answer in self.answers
        ]
        if len(set(coordinates)) != len(coordinates):
            raise ValueError("matrix answers repeat a partner/response coordinate")

        for axis_name in ("partner", "response"):
            labels: dict[str, str] = {}
            for answer in self.answers:
                axis = getattr(answer, axis_name)
                existing = labels.setdefault(axis.key, axis.label_en)
                if existing != axis.label_en:
                    raise ValueError(
                        f"matrix answers conflict on {axis_name} label {axis.key!r}"
                    )
        return self

    @property
    def columns(self) -> frozenset[str]:
        return frozenset(column for answer in self.answers for column in answer.columns)


def load_cis2016_matrix(
    path: Path | None, slug_dir: Path | None = None
) -> Cis2016Matrix | None:
    """Load the one reviewed CIS 2016 matrix declaration.

    Missing is an empty curation surface for wheel installs and synthetic
    builds, matching the repository's other curation loaders.  Syntax and
    contract drift are maintainer configuration errors with the standard
    ``EXIT_CONFIG`` shape.
    """
    if path is None or not path.is_file():
        return None
    try:
        matrix = Cis2016Matrix.model_validate_json(path.read_bytes())
    except (OSError, ValidationError) as exc:
        raise curation_error(
            _CODE,
            f"Could not load CIS 2016 matrix curation {path}: {exc}",
            f"Fix the selectors and evidence in reg_meta_build/{_FILE_NAME}.",
        ) from exc

    if slug_dir is not None:
        # The projection runs before DB slugs are populated. Reuse the same
        # curated-slug resolver as SCB errata so the human-readable FQIDs and
        # the numeric source selectors cannot silently disagree.
        from .scb_errata import _scb_slug_ids

        registers, variants = _scb_slug_ids(slug_dir)
        register_slug = matrix.selector.register_fqid.removeprefix("scb/")
        observed = (
            registers.get(register_slug),
            variants.get(f"{register_slug}/{matrix.selector.variant}"),
        )
        expected = (
            matrix.selector.register_id,
            matrix.selector.register_variant_id,
        )
        if observed != expected:
            raise curation_error(
                "cis2016_matrix_unknown_selector",
                "CIS 2016 matrix register/variant FQIDs do not resolve to their "
                f"declared source ids: expected {expected!r}, observed {observed!r}.",
                "Fix the selector or the curated SCB register/variant slugs; do "
                "not apply this evidence to another source coordinate.",
            )
    return matrix
