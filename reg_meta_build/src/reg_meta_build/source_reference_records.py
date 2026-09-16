"""Source declarations about native events, export columns and identifier keys.

These observations do not identify catalog entities or create relationships.
Native tokens retain their delivered representation and source-local meaning.
"""

# Pydantic resolves inherited declaration field types at runtime.
# ruff: noqa: TC001

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from reg_meta_build.source_records import (
    DeliveredCell,
    RecordLocator,
    SourceEvidenceTable,
    SourceField,
    SourceRevision,
)

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


class _SourceDeclaration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    revision: SourceRevision
    locator: RecordLocator
    delivered_cells: tuple[DeliveredCell, ...]


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


class SourceCodeOperand(BaseModel):
    """A named source code column; peer operands do not choose a namespace."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    role: Literal["input", "output", "peer"]
    name: str
    code: SourceField


class SourceCodeCrosswalkDeclaration(_SourceDeclaration):
    kind: Literal["code_crosswalk"] = "code_crosswalk"
    member_name: SourceField | None
    supplied_period: SourceField | None
    section_period: SourceField | None
    section_locator: RecordLocator | None
    description: SourceField | None
    operands: tuple[SourceCodeOperand, ...]


class SourceDerivationClause(BaseModel):
    """One named literal clause, never an evaluated expression or code list."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    name: str
    content: SourceField


class SourceDerivationDeclaration(_SourceDeclaration):
    kind: Literal["derivation"] = "derivation"
    member_name: SourceField | None
    supplied_period: SourceField | None
    description: SourceField | None
    clauses: tuple[SourceDerivationClause, ...]


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
    "SourceCodeCrosswalkDeclaration",
    "SourceCodeOperand",
    "SourceColumnTypeDeclaration",
    "SourceDerivationClause",
    "SourceDerivationDeclaration",
    "SourceEntityKind",
    "SourceEventAction",
    "SourceEventDeclaration",
    "SourceJoinKeyDeclaration",
    "SourceReferenceDeclaration",
]
