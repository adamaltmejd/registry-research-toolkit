"""Complete-scope composition checks curation before catalog dependencies: ordinary formation, delivery metadata, checked lists and copied coding."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from _csv_fixtures import SCB_REVISION
from _prepared_fixtures import accept_prepared
from _source_scope_support import acknowledge, guard, names, record, resolve
from reg_meta_build.curation_compile import (
    compile_scb_preliminary,
    convert_column_partitions,
)
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.prepared_values import (
    open_prepared_source_values,
    prepare_source_values,
)
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationCode,
)
from reg_meta_build.source_coding import copied_coding_fingerprints
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CuratedOccurrenceAddition,
    CurationCase,
    DeliveryMetadataColumn,
    DeliveryMetadataDecision,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    SourceEvidence,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    copied_coding_key,
    record_ref,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
)
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)
from reg_meta_build.source_value_bindings import bind_copied_coding, open_value_bindings
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
    SourceValueValidity,
    SourceValueWindow,
)

from reg_meta_build.fqid_slugs import SlugEntry


def test_ordinary_scope_forms_variables_with_literal_provider_keys():
    first, second = record(), record(2, variable=6, column="OTHER")
    result = resolve((first, second))
    assert result.evaluations == () and result.diagnostics == ()
    assert len(result.parents.registers) == len(result.parents.variants) == 1
    assert {v.provider_key for v in result.variables.values()} == {"5", "6"}
    assert sum(len(v.states) for v in result.variables.values()) == 2
    assert len(result.corrections.occurrences) == 2


def test_scope_routes_checked_delivery_metadata_to_literal_state_formation():
    records = tuple(
        item.model_copy(
            update={
                "fields": item.fields.model_copy(
                    update={
                        "definition": value_field("Annual received amount"),
                        "name": value_field("Annual amount (" + unit + ")"),
                        "measurement_unit": value_field(unit),
                    }
                )
            }
        )
        for item, unit in (
            (record(year="2020"), "kronor"),
            (record(member=2, year="2021"), "hundratals kronor"),
        )
    )
    case = CurationCase(
        case_id="checked-literal-delivery-names",
        peer_guards=(
            PeerGuard(
                guard_id="whole-variable",
                source=records[0].source,
                coordinates=(("variable", records[0].subject.variable),),
                expected_members=tuple(record_ref(item) for item in records),
            ),
        ),
        targets=capture_expectations(
            records, fields=tuple(SourceFields.model_fields), parents=True, coding=True
        ),
        decision=DeliveryMetadataDecision(
            fields=("name",),
            reviewed=True,
            variable_key=native_variable_key(records[0]),
            columns=(
                DeliveryMetadataColumn(
                    variant_key=native_variant_key(records[0]),
                    column="VALUE",
                    valid_from="2020-01-01",
                    valid_to="2021-12-31",
                    expected_codings=(),
                ),
            ),
            reason="Retain exact supplied delivery names without selecting one common name.",
            provenance="exact source fixture",
        ),
    )
    result = resolve(records, cases=(case,))
    assert {(issue.code, issue.severity) for issue in result.diagnostics} == {
        ("delivery_text_projected", "warning"),
        ("delivery_units_vary", "warning"),
    }
    variable = next(iter(result.variables.values()))
    assert variable.measurement_unit is None
    assert [
        (state.valid_from, state.measurement_unit) for state in variable.states
    ] == [
        ("2020-01-01", "kronor"),
        ("2021-01-01", "hundratals kronor"),
    ]
    assert (
        tuple(
            source
            for occurrence in result.corrections.occurrences
            for source in occurrence.source_records
        )
        == records
    )


def test_delivery_metadata_scope_uses_corrected_partition_identity():
    original = record()
    item = original.model_copy(
        update={
            "fields": original.fields.model_copy(
                update={
                    "description": value_field("Literal supplied delivery description")
                }
            )
        }
    )
    partition = convert_column_partitions(
        (item,), source_id="1.5", split_ids=("1.5.value",)
    )
    assert partition.case is not None
    split = partition.bindings[0].target.source_key
    metadata = CurationCase(
        case_id="partitioned-literal-metadata",
        targets=capture_expectations(
            (item,), fields=tuple(SourceFields.model_fields), parents=True, coding=True
        ),
        peer_guards=(guard(item),),
        decision=DeliveryMetadataDecision(
            reviewed=True,
            fields=("description",),
            variable_key=split,
            columns=(
                DeliveryMetadataColumn(
                    variant_key=native_variant_key(item),
                    column="VALUE",
                    valid_from="2020-01-01",
                    valid_to="2020-12-31",
                    source_scope=item.edition_period_scope,
                    expected_codings=(),
                ),
            ),
            reason="Retain literal metadata on its checked owner.",
            provenance="fixture",
        ),
    )
    naming = tuple(n for n in names((item,)) if n.target.kind != "variable") + (
        NamingDeclaration(
            target=partition.bindings[0].target,
            naming=SlugEntry(
                kind="variable", provider="scb", source_id="1.5.value", slug="value"
            ),
            contributors=(),
        ),
    )
    result = resolve(
        (item,),
        cases=(partition.case, metadata),
        naming=naming,
        provider_keys={split: "5.value"},
    )
    assert not any(
        d.code == "stale_delivery_metadata_scope" for d in result.diagnostics
    )
    assert {e.case_id: e.status for e in result.evaluations}[
        metadata.case_id
    ] == "applicable"
    assert split in result.variables
    assert result.corrections.occurrences[0].source_records == (item,)


def test_superseded_scb_preliminary_is_support_and_final_alone_forms_state():
    preliminary = record(
        edition_name="2020, preliminär version", edition_id=10, data_length="2"
    )
    preliminary_only = record(
        2,
        variable=6,
        column="OTHER",
        edition_name="2020, preliminär version",
        edition_id=10,
    )
    final = record(3, edition_name="2020, slutlig version", edition_id=11)
    records = (preliminary, preliminary_only, final)
    prepared = SimpleNamespace(
        records=SimpleNamespace(iter_records=lambda *, source: iter(records))
    )
    scope = CompiledScope(
        source=preliminary.source, register_key=None, naming=names(records)
    )
    (case,) = compile_scb_preliminary(cast("Any", prepared), (scope,))[
        preliminary.source, None
    ]
    result = resolve(records, cases=(case,))
    assert result.corrections.accounting[0].disposition == "applied"
    assert [item.use for item in result.corrections.occurrences] == [
        "support",
        "catalog",
        "catalog",
    ]
    assert result.corrections.occurrences[0].source_records == (preliminary,)
    shared = result.variables[native_variable_key(final)]
    exclusive = result.variables[native_variable_key(preliminary_only)]
    assert shared is not None and exclusive is not None
    assert len(shared.states) == len(exclusive.states) == 1
    assert shared.states[0].data_length == "1"
    assert exclusive.states[0].delivery_column_name == "OTHER"


def test_whole_list_item_validity_override_emits_one_warning(tmp_path):
    item = record(year="1990")
    root = tmp_path / "values"
    rows = (
        SourceValueAssociation(2, "list", "no", "values", member_id="1", item_id="1"),
        SourceValueAssociation(3, "list", "yes", "values", member_id="1", item_id="2"),
    )
    manifest = prepare_source_values(
        root,
        revision=SCB_REVISION,
        validity_revision=SCB_REVISION,
        descriptors=(SourceValueDescriptor("list"),),
        values=(SourceValue("no", "0", "No"), SourceValue("yes", "1", "Yes")),
        associations=rows,
        validity=tuple(
            SourceValueValidity(
                index + 2,
                str(index + 1),
                "2008-11-19",
                None,
                "validity",
                window=SourceValueWindow("known", "2008-11-19"),
            )
            for index in range(2)
        ),
        join=SourceValueJoin(
            record_sources=(SCB_REVISION.dataset,),
            member_target="native_member",
            member_format="integer",
            validity_target="item",
            missing_validity="unrestricted",
            rule="Exact fixture member relation",
            provenance=("fixture",),
        ),
    )
    source = open_prepared_source_values(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    with open_value_bindings((source,)) as sessions:
        result = resolve((item,), value_sessions=sessions)
    warnings = [
        diagnostic
        for diagnostic in result.diagnostics
        if diagnostic.code == "item_validity_set_aside"
    ]
    assert len(warnings) == 1
    assert warnings[0].severity == "warning"
    assert "list" in warnings[0].detail
    assert all(row.locator in warnings[0].detail for row in rows)
    assert not any(d.code == "empty_active_coding" for d in result.diagnostics)
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    assert variable.states[0].value_set.members == (("0", "No"), ("1", "Yes"))


def test_scope_forwards_label_rule_and_fqid_override(tmp_path):
    item = record()
    root = tmp_path / "values"
    manifest = prepare_source_values(
        root,
        revision=SCB_REVISION,
        validity_revision=SCB_REVISION,
        descriptors=(SourceValueDescriptor("list", version=" Listed "),),
        values=(SourceValue("value", "01", "One"),),
        associations=(
            SourceValueAssociation(
                1, "list", "value", "values", member_id="1", item_id="1"
            ),
        ),
        join=SourceValueJoin(
            record_sources=(SCB_REVISION.dataset,),
            member_target="native_member",
            member_format="integer",
            validity_target="item",
            missing_validity="unrestricted",
            rule="Exact fixture member relation",
            provenance=("fixture",),
        ),
    )
    source = open_prepared_source_values(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    first = ResolvedClassification(
        slug="first",
        short_name="FIRST",
        name="First",
        codes=(ResolvedClassificationCode(code="01", label="One"),),
    )
    second = first.model_copy(update={"slug": "second", "short_name": "SECOND"})
    with open_value_bindings((source,)) as sessions:
        ruled = resolve(
            (item,),
            value_sessions=sessions,
            classifications={"first": first, "second": second},
            label_rules={"Listed": "first"},
        )
        overridden = resolve(
            (item,),
            value_sessions=sessions,
            classifications={"first": first, "second": second},
            label_rules={"Listed": "first"},
            classification_overrides={
                "scb/example/value-5": (
                    "second",
                    "classifications/SECOND.toml#/binding/variable/1",
                )
            },
        )
    assert (
        ruled.variables[native_variable_key(item)]
        .states[0]
        .classification_links[0]
        .classification
        == "first"
    )
    assert (
        overridden.variables[native_variable_key(item)]
        .states[0]
        .classification_links[0]
        .classification
        == "second"
    )
    assert not ruled.diagnostics and not overridden.diagnostics


@pytest.mark.parametrize(
    "status,value,expected",
    [
        ("value", "New", (("N", "New meaning"),)),
        ("absent", None, None),
        ("unknown", None, None),
        ("value", "Missing", None),
    ],
)
def test_checked_list_declaration_controls_binding_without_replacing_evidence(
    tmp_path, status, value, expected
):
    item = record()
    item = item.model_copy(
        update={
            "fields": item.fields.model_copy(
                update={"value_set_declared": value_field("Old")}
            )
        }
    )
    case = CurationCase(
        case_id="correct-list",
        targets=capture_expectations((item,), fields=("value_set_declared",)),
        peer_guards=(guard(item),),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            reason="Checked list correction",
            provenance="fixture",
            effects=(
                CheckedFieldChange(
                    ref=record_ref(item),
                    replacement=FieldExpectation(
                        name="value_set_declared", status=status, value=value
                    ),
                ),
            ),
        ),
    )
    root = tmp_path / "values"
    manifest = prepare_source_values(
        root,
        revision=SCB_REVISION,
        descriptors=(
            SourceValueDescriptor("old", name="Old"),
            SourceValueDescriptor("new", name="New"),
        ),
        values=(
            SourceValue("old", "O", "Old meaning"),
            SourceValue("new", "N", "New meaning"),
        ),
        associations=(
            SourceValueAssociation(1, "old", "old", "values"),
            SourceValueAssociation(2, "new", "new", "values"),
        ),
        join=SourceValueJoin(
            record_sources=(SCB_REVISION.dataset,),
            member_target="declared_list",
            member_format="none",
            validity_target="row",
            missing_validity="unrestricted",
            rule="Exact declared fixture list",
            provenance=("fixture",),
        ),
    )
    source = open_prepared_source_values(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    with open_value_bindings((source,)) as sessions:
        result = resolve((item,), cases=(case,), value_sessions=sessions)
    variable = result.variables[native_variable_key(item)]
    assert variable is not None and len(variable.states) == 1
    codes = variable.states[0].value_set
    assert (codes.members if codes else None) == expected
    assert result.corrections.accounting[0].disposition == "applied"
    assert result.corrections.occurrences[0].source_records == (item,)
    assert item.fields.value_set_declared.value == "Old"
    assert [d.code for d in result.diagnostics] == (
        ["declared_value_list_not_found"] if value == "Missing" else []
    )


@pytest.mark.parametrize("change", ("code", "validity", "irrelevant_future"))
def test_copied_coding_checks_original_external_evidence_before_any_effects(
    tmp_path, change
):
    item = record()
    donor = record_ref(item)
    addition = CuratedOccurrenceAddition(
        occurrence_key="copied-2019",
        provider="scb",
        variable_key=native_variable_key(item),
        variant_key=native_variant_key(item),
        fields=item.fields,
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2019", end="2019"),)
        ),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        evidence=(donor,),
        donor=donor,
        copied_fields=tuple(SourceFields.model_fields),
        copy_coding=True,
        expected_codings=(),
    )
    case = CurationCase(
        case_id="checked-copy",
        targets=capture_expectations(
            (item,), fields=tuple(SourceFields.model_fields), coding=True
        ),
        peer_guards=(guard(item),),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            reason="Existing checked donor copy",
            provenance="fixture",
            effects=(addition,),
        ),
    )

    def prepare(name, *, code="01", start="2019-01-01", future=False):
        root = tmp_path / name / "values"
        rows = (
            SourceValueAssociation(
                1,
                "list",
                "code",
                "values",
                member_id="1",
                item_id="1",
                supplied_window=SourceValueWindow("known", start, "2020-12-31"),
            ),
        )
        if future:
            rows += (
                SourceValueAssociation(
                    2,
                    "list",
                    "future",
                    "values",
                    member_id="1",
                    item_id="2",
                    supplied_window=SourceValueWindow(
                        "known", "2021-01-01", "2021-12-31"
                    ),
                ),
            )
        manifest = prepare_source_values(
            root,
            revision=SCB_REVISION,
            validity_revision=SCB_REVISION,
            descriptors=(SourceValueDescriptor("list"),),
            values=(
                SourceValue("code", code, "Original label"),
                SourceValue("future", "99", "Future"),
            ),
            associations=rows,
            join=SourceValueJoin(
                record_sources=(SCB_REVISION.dataset,),
                member_target="native_member",
                member_format="integer",
                validity_target="item",
                missing_validity="unrestricted",
                rule="Exact fixture member",
                provenance=("fixture",),
            ),
        )
        return open_prepared_source_values(
            root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
        )

    with open_value_bindings((prepare("original"),)) as sessions:
        bound = bind_copied_coding(SourceEvidence((item,)), (case,), sessions)
        accepted = addition.model_copy(
            update={
                "expected_codings": copied_coding_fingerprints(
                    bound[copied_coding_key(addition)]
                )
            }
        )
        case = case.model_copy(
            update={
                "decision": case.decision.model_copy(update={"effects": (accepted,)})
            }
        )
        original = resolve((item,), cases=(case,), value_sessions=sessions)
    assert original.diagnostics == ()
    original_variable = original.variables[native_variable_key(item)]
    assert original_variable is not None
    assert [s.valid_from for s in original_variable.states] == [
        "2019-01-01",
        "2020-01-01",
    ]
    assert original.corrections.occurrences[-1].coding_records == (item,)

    changed = prepare(
        "changed",
        code="02" if change == "code" else "01",
        start="2020-01-01" if change == "validity" else "2019-01-01",
        future=change == "irrelevant_future",
    )
    with open_value_bindings((changed,)) as sessions:
        result = resolve((item,), cases=(case,), value_sessions=sessions)
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    if change == "irrelevant_future":
        assert result.diagnostics == ()
        assert variable == original_variable
    else:
        assert (
            result.evaluations[0].status
            == result.corrections.accounting[0].disposition
            == "stale"
        )
        assert [s.valid_from for s in variable.states] == ["2020-01-01"]
        assert [d.code for d in result.diagnostics] == [
            "copied_coding_evidence_changed"
        ]
        assert result.corrections.occurrences[0].source_records == (item,)
        assert len(result.corrections.occurrences) == 1


def test_ref_less_naming_issue_cannot_use_source_ack_exception():
    # Naming a native variable the scope never delivers is an ordinary error
    # without source refs; only an explicit source diagnostic names its register.
    item = record()
    absent = record(member=9, variable=9, column="ABSENT")
    naming = (
        *names((item,)),
        *(n for n in names((absent,)) if n.target.kind == "variable"),
    )
    (problem,) = resolve((item,), naming=naming).diagnostics
    assert problem.code == "naming_native_identity_missing"
    assert problem.refs == ()
    result = resolve((item,), naming=naming, cases=(acknowledge(problem, item),))
    assert not result.acknowledged
    assert problem in result.diagnostics
    assert any(d.code == "stale_curation_entry" for d in result.diagnostics)
