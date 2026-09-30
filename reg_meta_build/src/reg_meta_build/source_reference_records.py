"""Source declarations about native events, export columns and identifier keys.

These observations do not identify catalog entities or create relationships.
Native tokens retain their delivered representation and source-local meaning.
"""

# Pydantic resolves inherited declaration field types at runtime.
# ruff: noqa: TC001, TC002

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import Field
from reg_meta.documentary import (
    SourceCodeCrosswalkDeclaration,
    SourceDerivationDeclaration,
    _SourceDeclaration,
)
from reg_meta.source_evidence import DeliveredCell, SourceField, SourceRevision

from reg_meta_build.source_records import SourceEvidenceTable

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


class SourceColumnTypeDeclaration(_SourceDeclaration):
    kind: Literal["column_type"] = "column_type"
    table_name: SourceField
    column_name: SourceField
    data_type: SourceField
    declared_width: SourceField
    nullable: SourceField


class SourceJoinKeyDeclaration(_SourceDeclaration):
    kind: Literal["join_key"] = "join_key"
    table_name: SourceField
    column_name: SourceField
    description: SourceField


type SourceReferenceDeclaration = Annotated[
    SourceEventDeclaration
    | SourceColumnTypeDeclaration
    | SourceJoinKeyDeclaration
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
    "SourceColumnTypeDeclaration",
    "SourceEntityKind",
    "SourceEventAction",
    "SourceEventDeclaration",
    "SourceJoinKeyDeclaration",
    "SourceReferenceDeclaration",
]
