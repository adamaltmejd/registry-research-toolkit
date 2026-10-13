"""Source declarations about native events.

These observations do not identify catalog entities or create relationships.
Native tokens retain their delivered representation and source-local meaning.
"""

# Pydantic resolves inherited declaration field types at runtime.
# ruff: noqa: TC001

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import Field

from reg_meta_build.source_records import SourceEvidenceTable

from .documentary import (
    SourceCodeCrosswalkDeclaration,
    SourceDerivationDeclaration,
    _SourceDeclaration,
)
from .source_evidence import DeliveredCell, SourceField, SourceRevision

type SourceEventAction = Literal[
    "retired", "series_break", "replaced_by", "replaces", "unknown"
]
type SourceEntityKind = Literal[
    "register",
    "variant",
    "edition",
    "variable",
    "member",
    "population",
    "code_set",
    "population_context",
    "opaque",
]


class SourceEventDeclaration(_SourceDeclaration):
    kind: Literal["event"] = "event"
    name: SourceField
    action: SourceEventAction
    action_label: SourceField
    description: SourceField
    entity_kind: SourceEntityKind
    entity_label: SourceField
    first_token: DeliveredCell
    second_token: DeliveredCell
    document_token: DeliveredCell


type SourceReferenceDeclaration = Annotated[
    SourceEventDeclaration
    | SourceCodeCrosswalkDeclaration
    | SourceDerivationDeclaration,
    Field(discriminator="kind"),
]


@dataclass(frozen=True)
class CleanedSourceReferences:
    revision: SourceRevision
    declarations: tuple[SourceReferenceDeclaration, ...]
    tables: tuple[SourceEvidenceTable, ...]


__all__ = [
    "CleanedSourceReferences",
    "SourceEntityKind",
    "SourceEventAction",
    "SourceEventDeclaration",
    "SourceReferenceDeclaration",
]
