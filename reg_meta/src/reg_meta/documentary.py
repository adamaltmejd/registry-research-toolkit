"""Literal documentary source relationships; expressions are never evaluated."""

# Pydantic resolves the shared evidence model fields at runtime.
# ruff: noqa: TC001

from __future__ import annotations

import re
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from reg_meta.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceField,
    SourceRevision,
)


class _SourceDeclaration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    revision: SourceRevision
    locator: RecordLocator
    delivered_cells: tuple[DeliveredCell, ...]


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


type LiteralSourceRelationship = Annotated[
    SourceCodeCrosswalkDeclaration | SourceDerivationDeclaration,
    Field(discriminator="kind"),
]


class DocumentaryCoordinate(BaseModel):
    """An exact supplied clause or operand, without interpreting its contents."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    clause_index: int | None = Field(default=None, ge=0)
    operand_index: int | None = Field(default=None, ge=0)
    token: str = Field(min_length=1)

    @model_validator(mode="after")
    def _one_coordinate(self) -> Self:
        if (self.clause_index is None) == (self.operand_index is None):
            raise ValueError(
                "documentary reference needs exactly one clause or operand coordinate"
            )
        return self


def _variable_fqid(value: str) -> str:
    from reg_meta.fqid import FqidKind, parse

    if parse(value).kind != FqidKind.VARIABLE_BINDING:
        raise ValueError("documentary endpoint must be a catalog variable")
    return value


class DocumentaryVariableReference(DocumentaryCoordinate):
    variable: str

    _variable = field_validator("variable")(_variable_fqid)


class UnresolvedDocumentaryReference(DocumentaryCoordinate):
    reason: str = Field(min_length=1)


class DocumentaryRelationship(BaseModel):
    """Owner-bound literal metadata, with unresolved references made explicit."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    relationship_id: int = Field(gt=0)
    owner: str
    declaration: LiteralSourceRelationship
    variables: tuple[DocumentaryVariableReference, ...] = ()
    unresolved: tuple[UnresolvedDocumentaryReference, ...] = ()
    binding_status: Literal["owner_bound_literal"] = "owner_bound_literal"
    provenance: str = Field(min_length=1)

    _owner = field_validator("owner")(_variable_fqid)

    @model_validator(mode="after")
    def _coordinates(self) -> Self:
        member = self.declaration.member_name
        if (
            member is None
            or member.status != "value"
            or not isinstance(member.value, str)
            or not member.value
        ):
            raise ValueError("owner-bound literal requires a supplied member name")
        seen = set()
        for reference in (*self.variables, *self.unresolved):
            coordinate = (
                reference.clause_index,
                reference.operand_index,
                reference.token,
            )
            if coordinate in seen:
                raise ValueError("duplicate documentary reference coordinate")
            seen.add(coordinate)
            if reference.clause_index is not None:
                if not isinstance(
                    self.declaration, SourceDerivationDeclaration
                ) or reference.clause_index >= len(self.declaration.clauses):
                    raise ValueError(
                        "documentary clause coordinate is outside the supplied declaration"
                    )
                clause = self.declaration.clauses[reference.clause_index]
                value = clause.content.value
                if not isinstance(value, str) or reference.token not in value:
                    raise ValueError(
                        "documentary token is absent from its exact supplied clause"
                    )
                if isinstance(
                    reference, DocumentaryVariableReference
                ) and not re.search(
                    r"(?<!\w)" + re.escape(reference.token) + r"(?!\w)", value
                ):
                    raise ValueError(
                        "documentary variable token is not a literal supplied name"
                    )
            else:
                if (
                    not isinstance(self.declaration, SourceCodeCrosswalkDeclaration)
                    or reference.operand_index is None
                    or reference.operand_index >= len(self.declaration.operands)
                ):
                    raise ValueError(
                        "documentary operand coordinate is outside the supplied declaration"
                    )
                operand = self.declaration.operands[reference.operand_index]
                if reference.token != operand.name:
                    raise ValueError(
                        "documentary token disagrees with its exact supplied operand"
                    )
        return self
