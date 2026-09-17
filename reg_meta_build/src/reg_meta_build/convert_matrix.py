"""Offline conversion of the two accepted CIS answer declarations.

The common resolver consumes the resulting checked occurrences and names. It
does not load these legacy declarations or discover matrix identities at build
time. Pooled source editions retain their unresolved annual coverage.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.cis2016_matrix import Cis2014Matrix
from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.fqid_slugs import SlugEntry
from reg_meta_build.source_coordinates import source_register_key
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CheckedIdentityChange,
    CheckedSourceUse,
    CuratedOccurrenceAddition,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
)
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import NativeCoordinates, SourceFields, value_field

if TYPE_CHECKING:
    from reg_meta_build.cis2016_matrix import Cis2016Matrix
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import OccurrenceEffect
    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class MatrixConversion:
    case: CurationCase
    naming: tuple[NamingDeclaration, ...]
    provider_keys: dict[NativeKey, str]


def convert_matrix(
    matrix: Cis2014Matrix | Cis2016Matrix,
    records: tuple[SourceRecord, ...],
    *,
    case_id: str,
    provenance: str,
) -> MatrixConversion:
    """Bind an accepted answer partition to its complete original source scope.

    ``records`` must include every original member under the selected native
    register/variant/edition/variable, including competitors with another CVID.
    The emitted peer guard checks that same scope on replay. The caller retains
    the accepted declaration revision; expectations must never be refreshed during
    an ordinary build. Other editions and variables are left untouched.
    """
    selector = matrix.selector
    native = NativeCoordinates(
        register_id=selector.register_id,
        register_variant_id=selector.register_variant_id,
        edition_id=selector.regver_id,
        variable_id=selector.var_id,
    )
    selected = tuple(
        record
        for record in records
        if record.subject.provider == "scb"
        and all(
            getattr(record.subject.native, name) == value
            for name, value in native.model_dump(exclude_none=True).items()
        )
    )
    if not selected:
        raise ValueError("accepted matrix has no matching original source members")
    if len({record.source for record in selected}) != 1 or {
        record.subject.native.member_id for record in selected
    } != {selector.cvid}:
        raise ValueError("accepted matrix source or complete CVID partition changed")
    if any(record.edition_scope.label != selector.edition for record in selected):
        raise ValueError("accepted matrix edition label no longer matches its selector")
    blank = isinstance(matrix, Cis2014Matrix)
    columns = {
        field.value if field is not None and field.status == "value" else None
        for record in selected
        for field in (record.fields.column_name,)
    }
    if columns != ({None} if blank else matrix.columns):
        raise ValueError("accepted matrix complete literal column partition changed")
    expected = capture_expectations(
        selected, fields=tuple(SourceFields.model_fields), coding=True
    )
    if len(expected) != 1:
        raise ValueError("accepted matrix must identify one exact source member")
    donor = expected[0]
    if blank and len(donor.alternatives) != 1:
        raise ValueError("blank matrix donor has competing occurrence metadata")
    guard = PeerGuard(
        guard_id=f"{case_id}:complete-partition",
        source=selected[0].source,
        native=native,
        expected_members=(donor.ref,),
    )
    register_key = source_register_key(selected[0])
    original = source_occurrence(selected[0])
    if (
        register_key is None
        or original.variant_key is None
        or original.edition_key is None
    ):
        raise ValueError("accepted matrix needs established native parent identities")
    effects: list[OccurrenceEffect] = []
    if blank:
        effects.append(CheckedSourceUse(ref=donor.ref))
    names = []
    provider_keys = {}
    for answer in sorted(matrix.answers, key=lambda item: item.key):
        key = (*register_key, "accepted-matrix", case_id, answer.key)
        provider_keys[key] = str(selector.var_id)
        authored = {
            "name": value_field(answer.label_en),
            "definition": value_field(answer.definition_en),
            "description": value_field(
                f"{matrix.question_label} ({', '.join(answer.columns)}; {selector.edition})."
            ),
            "operational_definition": value_field(answer.definition_en),
        }
        for column in answer.columns:
            if blank:
                fields = selected[0].fields.model_copy(
                    update={**authored, "column_name": value_field(column)}
                )
                effects.append(
                    CuratedOccurrenceAddition(
                        occurrence_key=f"{case_id}:{answer.key}:{column}",
                        provider="scb",
                        variable_key=key,
                        variant_key=original.variant_key,
                        edition_key=original.edition_key,
                        population_key=original.population_key,
                        fields=fields,
                        edition_scope=original.edition_scope,
                        edition_period_scope=original.edition_period_scope,
                        evidence=(donor.ref,),
                        donor=donor.ref,
                        copied_fields=tuple(
                            name
                            for name in SourceFields.model_fields
                            if name not in {*authored, "column_name"}
                        ),
                        copy_coding=True,
                    )
                )
            else:
                when = (
                    FieldExpectation(name="column_name", status="value", value=column),
                )
                effects.append(
                    CheckedIdentityChange(ref=donor.ref, variable_key=key, when=when)
                )
                effects.extend(
                    CheckedFieldChange(
                        ref=donor.ref,
                        replacement=FieldExpectation(
                            name=name, status=field.status, value=field.value
                        ),
                        when=when,
                    )
                    for name, field in authored.items()
                )
        names.append(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="variable",
                    provider="scb",
                    source_key=key,
                    register_key=register_key,
                    expectations=expected,
                    peer_guards=(guard,),
                ),
                naming=SlugEntry(
                    kind="variable",
                    source_id=f"{case_id}:{answer.key}",
                    slug=answer.slug,
                    provider="scb",
                ),
                contributors=(),
            )
        )
    case = CurationCase(
        case_id=case_id,
        targets=expected,
        peer_guards=(guard,),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(effects),
            reason="Preserve the existing reviewed answer identities and exact source partition. "
            + matrix.evidence.question,
            provenance=provenance
            + "; accepted declaration="
            + matrix.model_dump_json(by_alias=True),
        ),
    )
    return MatrixConversion(case, tuple(names), provider_keys)
