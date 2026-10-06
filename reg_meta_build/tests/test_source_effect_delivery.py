"""Checked corrections: delivery statements and additional deliveries type columns from guarded evidence."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import TYPE_CHECKING

import pytest
from _source_effects_support import (
    effect_record as _record,
    effect_scope as _scope,
    storage_columns as _storage_columns,
)
from reg_meta.source_evidence import SourceField
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
)
from reg_meta_build.scb_errata import (
    ErrataDelivered,
    ErrataEditionBinding,
    ErrataVariantContext,
    convert_delivered_entry,
)
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CuratedOccurrenceAddition,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    CodeSetReference,
    SourceFields,
    SourceRecord,
    value_field,
)

if TYPE_CHECKING:
    from reg_meta_build.source_reference_records import SourceColumnTypeDeclaration


def _convert_delivered(
    records: tuple[SourceRecord, ...],
    target_edition: SourceRecord,
    *,
    native_variable_id: int | None = None,
    prefixes: tuple[str, ...] = (),
    storage_columns: dict[tuple[str, str], SourceColumnTypeDeclaration] | None = None,
    additional_physical_column: bool = False,
    storage_column: str | None = None,
):
    assert target_edition.original_period_text is not None
    target = source_occurrence(target_edition)
    assert target.edition_key is not None
    return convert_delivered_entry(
        ErrataDelivered(
            register_id=1,
            register_variant_id=2,
            column="VALUE",
            versions=(target_edition.original_period_text,),
            provenance="errata:accepted\nExisting evidence",
            native_variable_id=native_variable_id,
        ),
        case_id="accepted/delivered/0",
        context=ErrataVariantContext(
            records,
            1,
            2,
            (
                ErrataEditionBinding(
                    key=target.edition_key,
                    name=target_edition.original_period_text,
                    edition_scope=target.edition_scope,
                    edition_period_scope=target.edition_period_scope,
                    support=(record_ref(target_edition),),
                    native_id=target_edition.subject.native.edition_id,
                ),
            ),
        ),
        steward_table_prefixes=prefixes,
        storage_columns=storage_columns,
        additional_physical_column=additional_physical_column,
        storage_column=storage_column,
    )


@pytest.mark.parametrize("drift", ["field", "parent", "missing", "new"])
def test_additional_delivery_preserves_siblings_and_checks_complete_family(
    drift: str,
) -> None:
    donor = _record(cvid=10, column="VALUE", year="2022", data_type="integer")
    target = _record(cvid=21, column="OTHER")
    sibling = _record(cvid=22, column="SECOND")
    outside = _record(cvid=23, column="EARLIER", year="2018")
    records = donor, target, sibling, outside
    converted = _convert_delivered(
        records, target, native_variable_id=5, additional_physical_column=True
    )
    assert converted.case is not None and not converted.blockers
    applied = apply_occurrence_cases(records, (converted.case,))
    assert not applied.diagnostics
    assert tuple(o.fields.column_name.value for o in applied.occurrences[:-1]) == (
        "VALUE",
        "OTHER",
        "SECOND",
        "EARLIER",
    )
    addition = applied.occurrences[-1]
    assert addition.fields.column_name == value_field("VALUE")
    assert addition.fields.data_type is None
    assert addition.fields.operational_definition is None
    assert tuple(r.record_id for r in addition.evidence) == (donor.record_id,)
    if drift == "field":
        changed = outside.model_copy(
            update={
                "fields": outside.fields.model_copy(
                    update={"definition": value_field("Changed quantity")}
                )
            }
        )
        altered = donor, target, sibling, changed
    elif drift == "parent":
        altered = (
            donor,
            target,
            sibling,
            outside.model_copy(update={"parent_facts": ()}),
        )
    elif drift == "missing":
        altered = donor, target, sibling
    else:
        altered = *records, _record(cvid=24, column="NEW", year="2017")
    assert apply_occurrence_cases(altered, (converted.case,)).diagnostics


def test_additional_delivery_requires_anchor_and_refuses_present_column() -> None:
    from pydantic import ValidationError
    from reg_meta_build.curation_tree import ErrataDeliveredEntry

    with pytest.raises(ValidationError, match="requires native_variable_id"):
        ErrataDeliveredEntry(
            variant="people",
            column="VALUE",
            versions=["2020"],
            evidence="Exact physical holding",
            noted="2026-10-01",
            upstream="additional-physical-column-in-version",
        )
    donor = _record(cvid=10, column="VALUE", year="2022")
    present = _record(cvid=21, column="VALUE")
    with pytest.raises(ValueError, match="requires native_variable_id"):
        _convert_delivered((donor, present), present, additional_physical_column=True)
    result = _convert_delivered(
        (donor, present), present, native_variable_id=5, additional_physical_column=True
    )
    assert result.case is None
    assert any(x.startswith("now_present:") for x in result.blockers)


@pytest.mark.parametrize(
    "invalid", ["empty", "coordinate", "mixed_source", "foreign_support"]
)
def test_errata_context_validates_complete_variant_evidence(invalid: str) -> None:
    record = _record(column="VALUE")
    original = source_occurrence(record)
    assert original.edition_key is not None
    binding = ErrataEditionBinding(
        key=original.edition_key,
        name="2020",
        edition_scope=record.edition_scope,
        edition_period_scope=record.edition_period_scope,
        support=(record_ref(_record(cvid=99)),)
        if invalid == "foreign_support"
        else (record_ref(record),),
        native_id=2020,
    )
    with pytest.raises(ValueError):
        ErrataVariantContext(
            ()
            if invalid == "empty"
            else (record, record.model_copy(update={"source": "other-source"}))
            if invalid == "mixed_source"
            else (record,),
            99 if invalid == "coordinate" else 1,
            2,
            (binding,),
        )


def test_errata_context_preserves_physical_alternatives_and_edition_names() -> None:
    first = _record(column="VALUE")
    second = _record(column="value", data_type="varchar")
    other = _record(variable=99, cvid=30, column="OTHER")
    original = source_occurrence(first)
    assert original.edition_key is not None
    binding = ErrataEditionBinding(
        key=original.edition_key,
        name="2020",
        edition_scope=first.edition_scope,
        edition_period_scope=first.edition_period_scope,
        support=(record_ref(first),),
        native_id=2020,
    )
    records = (first, other, second, first)
    context = ErrataVariantContext(records, 1, 2, (binding, binding))
    assert context.records == records
    assert context.editions == (binding, binding)
    assert context.by_column["value"] == (first, second, first)
    assert context.records_for_refs({record_ref(first)}) == (first, second, first)
    for references in ({record_ref(first)}, {record_ref(other)}, set(context.by_ref)):
        assert context.expectations_for_refs(references) == capture_expectations(
            context.records_for_refs(references),
            fields=tuple(SourceFields.model_fields),
            parents=True,
            coding=True,
        )
    assert context.complete_expectations is context.complete_expectations
    changed = second.model_copy(
        update={
            "fields": second.fields.model_copy(update={"name": value_field("changed")})
        }
    )
    changed_context = ErrataVariantContext((first, other, changed), 1, 2, (binding,))
    assert changed_context.expectations_for_refs({record_ref(first)}) != (
        context.expectations_for_refs({record_ref(first)})
    )
    for field_name in ("records", "editions"):
        with pytest.raises(FrozenInstanceError):
            setattr(context, field_name, ())
    with pytest.raises(ValueError, match="register/variant"):
        convert_delivered_entry(
            ErrataDelivered(
                register_id=99,
                register_variant_id=2,
                column="VALUE",
                versions=("2020",),
                provenance="errata:accepted\nExisting evidence",
            ),
            case_id="wrong-coordinate",
            context=context,
        )


def test_converted_blank_target_sets_type_and_stays_source_guarded() -> None:
    donor = _record(column="Value", year="2022")
    blank = _record(cvid=21, year="2020", data_type="")
    result = _convert_delivered((donor, blank), blank)
    assert result.case is not None and result.blockers == ()
    applied = apply_occurrence_cases((donor, blank), (result.case,))
    assert applied.diagnostics == ()
    assert applied.occurrences[1].fields.column_name == SourceField(
        status="value", value="VALUE"
    )
    assert applied.occurrences[1].fields.data_type == SourceField(
        status="value", value="integer"
    )
    assert any(
        isinstance(effect, CheckedFieldChange)
        and effect.replacement.name == "data_type"
        for effect in result.case.decision.effects
    )
    assert applied.occurrences[1].source_records == (blank,)
    assert all(item.occurrence_key is None for item in applied.occurrences)
    # Unrelated variables are not dependencies; documented Datatyp is.
    unrelated = _record(variable=99, cvid=30, column="UNRELATED")
    assert (
        apply_occurrence_cases((donor, blank, unrelated), (result.case,)).diagnostics
        == ()
    )
    changed_donor = _record(column="Value", year="2022", data_type="varchar")
    assert (
        apply_occurrence_cases((changed_donor, blank), (result.case,))
        .accounting[0]
        .disposition
        == "stale"
    )


def test_delivery_statement_types_without_inheriting_flags_or_coding() -> None:
    before = _record(cvid=10, column="VALUE", year="2018", data_type="varchar")
    after = _record(cvid=20, column="VALUE", year="2022")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    records = before, after, edition
    result = _convert_delivered(records, edition)
    assert result.case is not None and result.blockers == ()
    assert set(result.identity_refs) == {record_ref(before), record_ref(after)}
    applied = apply_occurrence_cases(records, (result.case,))
    assert applied.diagnostics == ()
    added = applied.occurrences[-1]
    assert added.source_records == ()
    assert set(added.support_records) == {before, after}
    assert added.fields == SourceFields(
        availability=value_field(True),
        column_name=value_field("VALUE"),
        data_type=value_field("text"),
    )
    addition = result.case.decision.effects[0]
    assert isinstance(addition, CuratedOccurrenceAddition)
    assert addition.donor is None and addition.copied_fields == ()
    assert added.edition_scope == _scope("2020")
    assert record_ref(edition) in {item.ref for item in result.case.support}
    changed_edition = edition.model_copy(
        update={
            "fields": edition.fields.model_copy(update={"name": value_field("changed")})
        }
    )
    assert (
        apply_occurrence_cases((before, after, changed_edition), (result.case,))
        .accounting[0]
        .disposition
        == "stale"
    )
    # New source members require review; they never become automatic donors.
    nearer = _record(cvid=40, column="vAlUe", year="2021")
    changed = apply_occurrence_cases((*records, nearer), (result.case,))
    assert changed.accounting[0].disposition == "stale"
    assert all(item.occurrence_key is None for item in changed.occurrences)


@pytest.mark.parametrize("drift", ["prose", "parent", "coding"])
@pytest.mark.parametrize("role", ["donor", "target"])
def test_delivered_guards_complete_source_meaning_and_bindings(
    drift: str, role: str
) -> None:
    code = CodeSetReference(
        reference_id="declared-list",
        content_sha256="c" * 64,
        physical_locator="source:list:1",
    )
    donor = _record(column="VALUE", year="2022").model_copy(
        update={"code_set_references": (code,)}
    )
    target = _record(column="", year="2020").model_copy(
        update={"code_set_references": (code,)}
    )
    records = donor, target
    converted = _convert_delivered(records, target)
    assert converted.case is not None and converted.blockers == ()
    assert apply_occurrence_cases(records, (converted.case,)).diagnostics == ()
    original = donor if role == "donor" else target
    assert original.parent_facts
    changed = original.model_copy(
        update={
            "fields": original.fields.model_copy(
                update={"definition": value_field("Different measured quantity")}
            )
        }
        if drift == "prose"
        else {"parent_facts": ()}
        if drift == "parent"
        else {"code_set_references": ()}
    )
    replay = apply_occurrence_cases(
        tuple(changed if r is original else r for r in records),
        (converted.case,),
    )
    assert replay.accounting[0].disposition == "stale"
    assert all(o.fields == o.source_records[0].fields for o in replay.occurrences)


@pytest.mark.parametrize(
    ("documented", "storage", "expected"),
    [
        ("integer", "smallint", "integer"),
        ("integer", "varchar", "text"),
        ("", "smallint", "integer"),
        ("float", None, "decimal"),
        ("text", None, "text"),
        ("Datum och klockslag", "smallint", None),
        ("date", "smallint", None),
    ],
)
def test_delivered_type_widens_documentation_and_storage(
    documented: str, storage: str | None, expected: str | None
) -> None:
    donor = _record(column="VALUE", year="2022", data_type=documented)
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    columns = _storage_columns(("CIS2004", storage), column="VALUE") if storage else {}
    result = _convert_delivered(
        (donor, edition), edition, prefixes=("CIS",), storage_columns=columns
    )
    assert result.case is not None
    applied = apply_occurrence_cases((donor, edition), (result.case,))
    assert applied.diagnostics == ()
    added = next(item for item in applied.occurrences if item.occurrence_key)
    assert added.fields.data_type == (
        value_field(expected) if expected is not None else None
    )
    assert "Documented Datatyp: 2022=" in result.case.decision.provenance
    assert "SWECOV storage " in result.case.decision.provenance


def test_delivered_type_is_order_independent_across_records_and_storage() -> None:
    early = _record(cvid=10, column="VALUE", year="2018", data_type="integer")
    late = _record(cvid=20, column="value", year="2022", data_type="decimal")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    first = _storage_columns(
        ("CIS2004", "smallint"), ("CIS2012", "varchar"), column="VALUE"
    )
    reversed_storage = dict(reversed(tuple(first.items())))
    a = _convert_delivered(
        (early, late, edition), edition, prefixes=("CIS",), storage_columns=first
    )
    b = _convert_delivered(
        (edition, late, early),
        edition,
        prefixes=("CIS",),
        storage_columns=reversed_storage,
    )
    assert a.case is not None and b.case is not None
    assert a.case.decision.provenance == b.case.decision.provenance
    assert a.case.decision.effects == b.case.decision.effects
    assert "CIS2004=smallint, CIS2012=varchar" in a.case.decision.provenance


def test_delivered_without_evidence_stays_untyped_and_names_missing_evidence() -> None:
    donor = _record(column="VALUE", year="2022", data_type="")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    result = _convert_delivered((donor, edition), edition)
    assert result.case is not None
    applied = apply_occurrence_cases((donor, edition), (result.case,))
    addition = next(item for item in applied.occurrences if item.occurrence_key)
    assert addition.fields.data_type is None
    assert addition.variant_key is not None and addition.column_key is not None
    formed = form_native_variable(
        (addition,),
        register=ResolvedRegister(provider="scb", slug="fixture", name="Fixture"),
        variants={addition.variant_key: ResolvedVariant(slug="people", name="People")},
        slug="value",
        provider_key="2.value",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding={addition.column_key: resolve_code_membership(())},
    )
    details = [
        issue.detail
        for issue in formed.diagnostics
        if issue.code == "unknown_data_type"
    ]
    assert len(details) == 1
    assert "SWECOV storage " in details[0] and ": none" in details[0]
    assert "Documented Datatyp: 2022=none" in details[0]


def test_delivery_statement_cannot_choose_between_reused_column_identities() -> None:
    before = _record(cvid=10, column="VALUE", year="2018")
    after = _record(cvid=20, variable=6, column="VALUE", year="2022")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    result = _convert_delivered((before, after, edition), edition)
    assert result.case is None
    assert result.blockers == ("ambiguous_documented_column_identity",)
    assert set(result.identity_refs) == {record_ref(before), record_ref(after)}


def test_delivered_anchor_selects_one_reused_column_identity() -> None:
    before = _record(cvid=10, column="VALUE", year="2018")
    after = _record(cvid=20, variable=6, column="VALUE", year="2022", data_type="text")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    records = before, after, edition
    result = _convert_delivered(records, edition, native_variable_id=5)
    assert result.case is not None and result.blockers == ()
    assert result.identity_refs == (record_ref(before),)
    applied = apply_occurrence_cases(records, (result.case,))
    assert applied.diagnostics == ()
    assert (
        applied.occurrences[-1].variable_key == source_occurrence(before).variable_key
    )
    assert applied.occurrences[-1].fields.data_type == value_field("integer")


def test_delivered_anchor_missing_under_column_is_stale() -> None:
    before = _record(cvid=10, column="VALUE", year="2018")
    after = _record(cvid=20, variable=6, column="VALUE", year="2022")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    result = _convert_delivered(
        (before, after, edition), edition, native_variable_id=99
    )
    assert result.case is None
    assert result.blockers == ("native_variable_id_not_documented_for_column",)
    assert set(result.identity_refs) == {record_ref(before), record_ref(after)}


@pytest.mark.parametrize(
    "column, extra, blocker",
    [
        ("VALUE", False, "now_present"),
        ("OTHER", False, "target_under_other_column"),
        ("", True, "ambiguous_target"),
    ],
)
def test_conversion_reports_original_evidence_conflicts(
    column: str, extra: bool, blocker: str
) -> None:
    donor = _record(column="VALUE", year="2022")
    target = _record(cvid=21, column=column)
    records = (donor, target, _record(cvid=22)) if extra else (donor, target)
    result = _convert_delivered(records, target)
    assert result.case is None
    assert any(item.startswith(blocker + ":") for item in result.blockers)


@pytest.mark.parametrize("header", ["PHYSICAL", "WRONG", None, "", " PHYSICAL"])
def test_additional_delivery_uses_only_explicit_own_storage_header(header):
    donor = _record(cvid=10, column="VALUE", year="2022", data_type="integer")
    target = _record(cvid=21, column="OTHER")
    if header in {"", " PHYSICAL"}:
        with pytest.raises(ValueError, match="trimmed nonempty header"):
            _convert_delivered(
                (donor, target),
                target,
                native_variable_id=5,
                additional_physical_column=True,
                storage_column=header,
            )
        return
    converted = _convert_delivered(
        (donor, target),
        target,
        native_variable_id=5,
        additional_physical_column=True,
        storage_column=header,
        prefixes=("CIS",),
        storage_columns=_storage_columns(("CIS2020", "varchar"), column="PHYSICAL"),
    )
    if header == "WRONG":
        assert converted.case is None
        assert converted.blockers == ("unsupported_storage_header",)
    else:
        assert converted.case is not None
        added = apply_occurrence_cases((donor, target), (converted.case,)).occurrences[
            -1
        ]
        assert (added.fields.data_type.value if added.fields.data_type else None) == (
            "text" if header else None
        )
        assert added.fields.operational_definition is None
        assert not added.coding_records
