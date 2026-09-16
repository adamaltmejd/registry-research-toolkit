"""Offline conversion of accepted SCB delivery omissions to checked common cases.

This is a conversion tool, not a build-time SCB correction pass. The nearest-edition
selection reproduces the old accepted rule once. The result records exact donors,
copied facts, target editions and all competing members; updates never rerun the rule.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build._curation import fold_column
from reg_meta_build.edition_bounds import edition_claims
from reg_meta_build.source_curation import (
    CheckedFieldChange,
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
from reg_meta_build.source_records import NativeCoordinates, canonical_sha256

if TYPE_CHECKING:
    from reg_meta_build.scb_errata import ErrataDelivered
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import OccurrenceEffect, SourceRecordRef
    from reg_meta_build.source_records import SourceRecord, TemporalScope


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
class DeliveredConversion:
    case: CurationCase | None
    blockers: tuple[str, ...]
    donor_refs: tuple[SourceRecordRef, ...]


def capture_expectations(
    records: tuple[SourceRecord, ...], *, fields: tuple[str, ...], parents: bool = False
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


def _donor_rank(record: SourceRecord, target: str) -> tuple[int, int, int, int]:
    """Exact legacy nearest-edition rule, used only when converting accepted input."""
    target_years = tuple(year for year, _lo, _hi in edition_claims(target))
    years = tuple(
        year for year, _lo, _hi in edition_claims(record.original_period_text or "")
    )
    edition_id = record.subject.native.edition_id
    if edition_id is None:
        raise ValueError("SCB delivery conversion requires native edition IDs")
    if not target_years or not years:
        return 1, 0, 0, edition_id
    return (
        0,
        min(abs(year - target_year) for year in years for target_year in target_years),
        0 if years[-1] >= target_years[-1] else 1,
        edition_id,
    )


def convert_delivered_entry(
    entry: ErrataDelivered,
    *,
    case_id: str,
    records: tuple[SourceRecord, ...],
    editions: tuple[ErrataEditionBinding, ...],
) -> DeliveredConversion:
    """Convert one old entry using the complete original native variant slice.

    Missing conversion bindings are engineering errors. Contradictory original
    evidence is an explicit blocked conversion. No latest/largest coding winner or
    resolved legacy database content is used as source evidence.
    """
    if not records or len({record.source for record in records}) != 1:
        raise ValueError("conversion requires a complete single-source variant slice")
    if any(
        record.subject.provider != "scb"
        or record.subject.native.register_id != entry.register_id
        or record.subject.native.register_variant_id != entry.register_variant_id
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
        return DeliveredConversion(None, ("no_documented_donor_column",), ())
    effects: list[OccurrenceEffect] = []
    targets = set()
    donors = set()
    blockers = []
    named_targets = set()
    peer_guards = {
        "donors": PeerGuard(
            guard_id=f"{case_id}:donors",
            source=records[0].source,
            native=NativeCoordinates(
                register_id=entry.register_id,
                register_variant_id=entry.register_variant_id,
            ),
            folded_column=fold_column(entry.column),
            expected_members=tuple(
                sorted({record_ref(record) for record in candidates}, key=str)
            ),
        )
    }
    for name in entry.versions:
        nearest = min(
            candidates, key=lambda record: _donor_rank(record, name)
        ).subject.native.edition_id
        selected = tuple(
            record
            for record in candidates
            if record.subject.native.edition_id == nearest
        )
        for edition in (item for item in editions if item.name == name):
            if edition.native_id is not None and any(
                record.subject.native.edition_id == edition.native_id
                for record in candidates
            ):
                blockers.append(f"now_present:{name}")
                continue
            for donor in selected:
                donor_ref = record_ref(donor)
                donors.add(donor_ref)
                column = _text(donor, "column_name")
                assert column is not None
                native = donor.subject.native
                matching = tuple(
                    record
                    for record in records
                    if edition.native_id is not None
                    and record.subject.native.edition_id == edition.native_id
                    and record.subject.native.variable_id == native.variable_id
                )
                members = {record.subject.native.member_id for record in matching}
                guard_key = (
                    f"{name}:edition:{edition.native_id}:variable:{native.variable_id}"
                )
                guarded = (
                    matching
                    if edition.native_id is not None
                    else tuple(
                        record
                        for record in records
                        if record.subject.native.variable_id == native.variable_id
                        and record.edition_scope == edition.edition_scope
                    )
                )
                peer_guards[guard_key] = PeerGuard(
                    guard_id=f"{case_id}:{guard_key}",
                    source=donor.source,
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
                        sorted({record_ref(record) for record in guarded}, key=str)
                    ),
                )
                if len(members) > 1:
                    blockers.append(
                        f"ambiguous_target:{name}:variable:{native.variable_id}"
                    )
                    continue
                if any(_text(record, "column_name") for record in matching):
                    blockers.append(
                        f"target_under_other_column:{name}:variable:{native.variable_id}"
                    )
                    continue
                if matching:
                    for target in matching:
                        ref = record_ref(target)
                        if (ref, column) in named_targets:
                            continue
                        targets.add(ref)
                        named_targets.add((ref, column))
                        effects.append(
                            CheckedFieldChange(
                                ref=ref,
                                replacement=FieldExpectation(
                                    name="column_name", status="value", value=column
                                ),
                            )
                        )
                    continue
                original = source_occurrence(donor)
                if original.variable_key is None or original.variant_key is None:
                    raise ValueError("donor is missing native topology")
                targets.add(donor_ref)
                support = tuple(
                    sorted(
                        {donor_ref, *edition.support},
                        key=lambda ref: (ref.source, ref.semantic_record_key),
                    )
                )
                # Variable-grain prose remains donor support. An addition preserves
                # the donor facts without claiming they were physically redelivered.
                copied = tuple(
                    name
                    for name in type(donor.fields).model_fields
                    if getattr(donor.fields, name) is not None
                )
                effects.append(
                    CuratedOccurrenceAddition(
                        occurrence_key=f"{case_id}:{canonical_sha256([list(edition.key), donor_ref.model_dump(mode='json')])}",
                        provider="scb",
                        variable_key=original.variable_key,
                        variant_key=original.variant_key,
                        edition_key=edition.key,
                        # The legacy clone carried no population claim into the target.
                        population_key=None,
                        fields=donor.fields,
                        edition_scope=edition.edition_scope,
                        edition_period_scope=edition.edition_period_scope,
                        evidence=support,
                        donor=donor_ref,
                        copied_fields=copied,
                    )
                )
    if blockers:
        return DeliveredConversion(
            None, tuple(sorted(set(blockers))), tuple(sorted(donors, key=str))
        )
    # Pin relevant donor/target facts and edition evidence. Unrelated variables in
    # the same variant are review context, not dependencies of this correction.
    all_fields = tuple(type(records[0].fields).model_fields)
    candidate_refs = {record_ref(record) for record in candidates}
    required_refs = (
        targets
        | candidate_refs
        | {
            ref
            for edition in editions
            if edition.name in entry.versions
            for ref in edition.support
        }
    )
    selected_records = defaultdict(list)
    for record in records:
        if (ref := record_ref(record)) in required_refs:
            selected_records[ref].append(record)
    expected = tuple(
        expectation
        for ref, selected_records_for_ref in sorted(
            selected_records.items(), key=lambda item: str(item[0])
        )
        for expectation in capture_expectations(
            tuple(selected_records_for_ref),
            fields=all_fields
            if ref in candidate_refs
            else ("column_name",)
            if ref in targets
            else ("availability",),
        )
    )
    case = CurationCase(
        case_id=case_id,
        targets=tuple(item for item in expected if item.ref in targets),
        support=tuple(item for item in expected if item.ref not in targets),
        peer_guards=tuple(peer_guards[key] for key in sorted(peer_guards)),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(effects),
            reason=entry.provenance,
            provenance=entry.provenance,
        ),
    )
    return DeliveredConversion(case, (), tuple(sorted(donors, key=str)))
