"""Offline conversion of accepted SCB delivery omissions to checked common cases.

This is a conversion tool, not a build-time SCB correction pass. An accepted
delivery statement establishes its exact column and edition membership. It does
not establish another edition's type, coding or other occurrence metadata.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build._curation import fold_column
from reg_meta_build.normalization import normalize_text
from reg_meta_build.source_coordinates import native_variant_key, source_register_key
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CodeSetExpectation,
    CuratedOccurrenceAddition,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    parent_fact_projection,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceFields,
    TemporalScope,
    canonical_sha256,
    value_field,
)

if TYPE_CHECKING:
    from reg_meta_build.scb_errata import ErrataColumn, ErrataDelivered
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import OccurrenceEffect, SourceRecordRef
    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class ErrataEditionBinding:
    """Already-established edition, including an existing declared missing edition."""

    key: NativeKey
    name: str
    edition_scope: TemporalScope
    edition_period_scope: TemporalScope
    # Existing editions use their member rows; declared editions use the finite
    # native variant evidence checked by the separate edition declaration.
    support: tuple[SourceRecordRef, ...]
    native_id: int | None = None


@dataclass(frozen=True)
class ErrataConversion:
    case: CurationCase | None
    blockers: tuple[str, ...]
    identity_refs: tuple[SourceRecordRef, ...]


def capture_expectations(
    records: tuple[SourceRecord, ...],
    *,
    fields: tuple[str, ...],
    parents: bool = False,
    coding: bool = False,
) -> tuple[RecordExpectation, ...]:
    """Capture a finite conversion baseline; never call this to refresh stale cases."""
    grouped = defaultdict(dict)
    for record in records:
        projection = RecordProjection(
            fields=tuple(
                FieldExpectation(name=name, status="absent")
                if field is None
                else FieldExpectation(name=name, status=field.status, value=field.value)
                for name in fields
                for field in (getattr(record.fields, name),)
            ),
            subject=record.subject,
            edition_scope=record.edition_scope,
            edition_period_scope=record.edition_period_scope,
            code_set_references=tuple(
                CodeSetExpectation(
                    reference_id=ref.reference_id,
                    content_sha256=ref.content_sha256,
                )
                for ref in record.code_set_references
            )
            if coding
            else None,
            parent_facts=tuple(
                parent_fact_projection(parent) for parent in record.parent_facts
            )
            if parents
            else None,
        )
        token = canonical_sha256(projection.model_dump(mode="json"))
        grouped[record_ref(record)][token] = projection
    return tuple(
        RecordExpectation(
            ref=ref, alternatives=tuple(items[key] for key in sorted(items))
        )
        for ref, items in sorted(
            grouped.items(),
            key=lambda item: (item[0].source, item[0].semantic_record_key),
        )
    )


def _text(record: SourceRecord, field: str) -> str | None:
    claim = getattr(record.fields, field)
    return claim.value if claim is not None and claim.status == "value" else None


def _variant_records(
    records: tuple[SourceRecord, ...],
    register_id: int,
    variant_id: int,
    editions: tuple[ErrataEditionBinding, ...],
) -> None:
    if not records or len({record.source for record in records}) != 1:
        raise ValueError("conversion requires a complete single-source variant slice")
    if any(
        record.subject.provider != "scb"
        or record.subject.native.register_id != register_id
        or record.subject.native.register_variant_id != variant_id
        for record in records
    ):
        raise ValueError(
            "source slice differs from the accepted errata register/variant"
        )
    refs = {record_ref(record) for record in records}
    if any(
        not edition.support or not set(edition.support) <= refs for edition in editions
    ):
        raise ValueError(
            "edition bindings require checked evidence in the supplied slice"
        )


def convert_delivered_entry(
    entry: ErrataDelivered,
    *,
    case_id: str,
    records: tuple[SourceRecord, ...],
    editions: tuple[ErrataEditionBinding, ...],
) -> ErrataConversion:
    """Retain the availability stated by an accepted omission declaration.

    An elsewhere-documented column must identify one source-native variable.
    Its original records support identity only: proximity in time never proves
    missing-edition metadata or code membership. Blank targets keep their own
    facts; completely absent occurrences carry only the declared delivery facts.
    """
    _variant_records(records, entry.register_id, entry.register_variant_id, editions)
    for name in entry.versions:
        if not any(edition.name == name for edition in editions):
            raise ValueError(f"missing edition conversion binding: {name!r}")
    candidates = tuple(
        record
        for record in records
        if (column := _text(record, "column_name"))
        and fold_column(column) == fold_column(entry.column)
    )
    if not candidates:
        return ErrataConversion(None, ("no_documented_column_identity",), ())
    references = tuple(sorted({record_ref(record) for record in candidates}, key=str))
    identities = {source_occurrence(record).variable_key for record in candidates}
    if None in identities or len(identities) != 1:
        return ErrataConversion(
            None, ("ambiguous_documented_column_identity",), references
        )
    original = source_occurrence(candidates[0])
    assert original.variable_key is not None and original.variant_key is not None
    native = candidates[0].subject.native
    effects: list[OccurrenceEffect] = []
    targets: set[SourceRecordRef] = set()
    blockers = []
    guards = [
        PeerGuard(
            guard_id=f"{case_id}:documented-column",
            source=records[0].source,
            native=NativeCoordinates(
                register_id=entry.register_id,
                register_variant_id=entry.register_variant_id,
            ),
            folded_column=fold_column(entry.column),
            expected_members=references,
        )
    ]
    for edition in (item for item in editions if item.name in entry.versions):
        if edition.native_id is not None and any(
            record.subject.native.edition_id == edition.native_id
            for record in candidates
        ):
            blockers.append(f"now_present:{edition.name}")
            continue
        matching = tuple(
            record
            for record in records
            if record.subject.native.variable_id == native.variable_id
            and (
                record.subject.native.edition_id == edition.native_id
                if edition.native_id is not None
                else record.edition_scope == edition.edition_scope
            )
        )
        guards.append(
            PeerGuard(
                guard_id=f"{case_id}:{edition.name}:variable:{native.variable_id}",
                source=records[0].source,
                native=NativeCoordinates(
                    register_id=entry.register_id,
                    register_variant_id=entry.register_variant_id,
                    edition_id=edition.native_id,
                    variable_id=native.variable_id,
                ),
                edition_scopes=()
                if edition.native_id is not None
                else (edition.edition_scope,),
                expected_members=tuple(
                    sorted({record_ref(record) for record in matching}, key=str)
                ),
            )
        )
        if len({record.subject.native.member_id for record in matching}) > 1:
            blockers.append(
                f"ambiguous_target:{edition.name}:variable:{native.variable_id}"
            )
            continue
        if any(_text(record, "column_name") for record in matching):
            blockers.append(
                f"target_under_other_column:{edition.name}:variable:{native.variable_id}"
            )
            continue
        if matching:
            for ref in sorted({record_ref(record) for record in matching}, key=str):
                targets.add(ref)
                effects.append(
                    CheckedFieldChange(
                        ref=ref,
                        replacement=FieldExpectation(
                            name="column_name", status="value", value=entry.column
                        ),
                    )
                )
            continue
        targets.update(references)
        effects.append(
            CuratedOccurrenceAddition(
                occurrence_key=f"{case_id}:{canonical_sha256([list(edition.key), entry.column])}",
                provider="scb",
                variable_key=original.variable_key,
                variant_key=original.variant_key,
                edition_key=edition.key,
                fields=SourceFields(
                    availability=value_field(True),
                    column_name=value_field(entry.column),
                ),
                edition_scope=edition.edition_scope,
                edition_period_scope=edition.edition_period_scope,
                evidence=tuple(sorted({*references, *edition.support}, key=str)),
            )
        )
    if blockers:
        return ErrataConversion(None, tuple(sorted(set(blockers))), references)
    required = (
        targets
        | set(references)
        | {
            ref
            for edition in editions
            if edition.name in entry.versions
            for ref in edition.support
        }
    )
    # Type, coding and other adjacent-edition facts are not dependencies because
    # this declaration does not assert them. Preserve only the identity evidence.
    expected = capture_expectations(
        tuple(record for record in records if record_ref(record) in required),
        fields=("column_name",),
    )
    case = CurationCase(
        case_id=case_id,
        targets=tuple(item for item in expected if item.ref in targets),
        support=tuple(item for item in expected if item.ref not in targets),
        peer_guards=tuple(guards),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(effects),
            reason=entry.provenance,
            provenance=entry.provenance,
        ),
    )
    return ErrataConversion(case, (), references)


def convert_column_entry(
    entry: ErrataColumn,
    *,
    case_id: str,
    records: tuple[SourceRecord, ...],
    editions: tuple[ErrataEditionBinding, ...],
    declared_flags: frozenset[str],
) -> ErrataConversion:
    """Retain supplied column facts without legacy flags or coverage defaults.

    The old ``all_versions`` interpretation of undated holdings supplies no annual
    evidence. Retain one undated occurrence, not a claim for each existing edition.
    ``declared_flags`` names the keys actually present in the original TOML, since
    the legacy loader has already replaced omitted flags with false.
    Classification and presentation declarations must be bound by the caller.
    """
    if not declared_flags <= {"is_identifier", "is_sensitive"}:
        raise ValueError("declared_flags must name original boolean declaration keys")
    _variant_records(records, entry.register_id, entry.register_variant_id, editions)
    if any(
        (column := _text(record, "column_name"))
        and fold_column(column) == fold_column(entry.column)
        for record in records
    ):
        return ErrataConversion(None, ("column_now_documented",), ())
    names = set(entry.versions or ())
    missing = names - {edition.name for edition in editions}
    if missing or entry.versions == ():
        raise ValueError(
            f"missing column edition conversion bindings: {sorted(missing)!r}"
        )
    selected = tuple(edition for edition in editions if edition.name in names)
    references = {ref for edition in selected for ref in edition.support}
    if entry.versions is None:
        # This member establishes the native variant coordinate only. It says
        # nothing about when the independently declared column was delivered.
        references.add(record_ref(records[0]))
    anchors = tuple(record for record in records if record_ref(record) in references)
    expected = capture_expectations(anchors, fields=("availability",))
    guards = [
        PeerGuard(
            guard_id=f"{case_id}:column-absent",
            source=records[0].source,
            native=NativeCoordinates(
                register_id=entry.register_id,
                register_variant_id=entry.register_variant_id,
            ),
            folded_column=fold_column(entry.column),
            expected_members=(),
        )
    ]
    for ref in sorted(references, key=str):
        anchor = next(record for record in anchors if record_ref(record) == ref)
        guards.append(
            PeerGuard(
                guard_id=f"{case_id}:edition:{ref.semantic_record_key!r}",
                source=anchor.source,
                native=anchor.subject.native,
                expected_members=(ref,),
            )
        )
    register = source_register_key(records[0])
    variant = native_variant_key(records[0])
    assert register is not None and variant is not None
    variable = (*register, "declared-column", fold_column(entry.column))
    fields = SourceFields(
        availability=value_field(True),
        column_name=value_field(entry.column),
        name=value_field(normalize_text(entry.name)),
        description=value_field(normalize_text(entry.definition, multiline=True)),
        data_type=value_field(entry.data_type) if entry.data_type is not None else None,
        identifier=value_field(entry.is_identifier)
        if "is_identifier" in declared_flags
        else None,
        sensitivity=value_field(entry.is_sensitive)
        if "is_sensitive" in declared_flags
        else None,
    )
    effects: tuple[OccurrenceEffect, ...] = tuple(
        CuratedOccurrenceAddition(
            occurrence_key=f"{case_id}:{canonical_sha256(list(edition.key))}",
            provider="scb",
            variable_key=variable,
            variant_key=variant,
            edition_key=edition.key,
            fields=fields,
            edition_scope=edition.edition_scope,
            edition_period_scope=edition.edition_period_scope,
            evidence=edition.support,
        )
        for edition in selected
    )
    if entry.versions is None:
        effects = (
            CuratedOccurrenceAddition(
                occurrence_key=f"{case_id}:undated",
                provider="scb",
                variable_key=variable,
                variant_key=variant,
                edition_key=None,
                fields=fields,
                edition_scope=TemporalScope(
                    kind="unknown",
                    label="Undated holdings; legacy all_versions is not annual evidence",
                ),
                edition_period_scope=TemporalScope(kind="not_applicable"),
                evidence=tuple(sorted(references, key=str)),
            ),
        )
    case = CurationCase(
        case_id=case_id,
        targets=expected,
        peer_guards=tuple(guards),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=effects,
            reason=entry.provenance,
            provenance=entry.provenance,
        ),
    )
    return ErrataConversion(case, (), ())
