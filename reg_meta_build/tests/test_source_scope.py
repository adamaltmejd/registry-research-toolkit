"""Complete-scope composition checks curation before catalog dependencies."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, cast

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from _prepared_fixtures import accept_prepared
from reg_meta.source_evidence import SourceField, SourceRevision, canonical_sha256
from reg_meta_build.catalog_dependencies import (
    CatalogDependencies,
    CatalogDependencyError,
    CoverageObligation,
    check_delivery_coverage,
    resolve_panel_dependencies,
    variable_dependency_keys,
)
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
    ResolvedVariant,
)
from reg_meta_build.source_coding import copied_coding_fingerprints
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_parent_key,
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    AliasWindowDecision,
    CheckedFieldChange,
    CheckedIdentityChange,
    ClassificationDecision,
    CodingDecision,
    CuratedOccurrenceAddition,
    CurationCase,
    DeliveryUnitColumn,
    DeliveryUnitDecision,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    ResolutionDiagnostic,
    SearchAliasDecision,
    SourceEvidence,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    copied_coding_key,
    record_ref,
)
from reg_meta_build.source_naming import (
    AcceptedNamingEntry,
    NamingAmbiguity,
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceFields,
    SourceRecord,
    TemporalScope,
    value_field,
)
from reg_meta_build.source_scope import resolve_source_scope
from reg_meta_build.source_siblings import SiblingResolution
from reg_meta_build.source_support import SourceSupportBindings
from reg_meta_build.source_value_bindings import bind_copied_coding, open_value_bindings
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
    SourceValueValidity,
    SourceValueWindow,
)
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

REVISION = SourceRevision.create(
    dataset="scope-fixture",
    publisher="SCB",
    purpose="Scope composition",
    upstream_revision="1",
    artifact_path="records.csv",
    artifact_size=1,
    artifact_sha256="a" * 64,
)


def record(
    member=1,
    variable=5,
    variant=2,
    column="VALUE",
    year="2020",
    register_id=1,
    edition_name=None,
    edition_id=None,
    data_length="1",
):
    header = REGISTERINFORMATION_HEADER.split("|")
    values = _var_row(
        cvid=member,
        var_id=variable,
        colname=column,
        register=("TEST", register_id, variant),
        versionname=edition_name,
        regver_id=int(year) if edition_id is None else edition_id,
        data_length=data_length,
        year=year,
    ).split("|")
    result = clean_scb_row(
        header,
        member,
        {
            name: (True, value, value)
            for name, value in zip(header, values, strict=True)
        },
        REVISION,
    ).record
    return result.model_copy(
        update={
            "fields": result.fields.model_copy(
                update={
                    "identifier": value_field(False),
                    "sensitivity": value_field(False),
                }
            )
        }
    )


def names(records):
    grouped = defaultdict(list)
    for item in records:
        grouped["variable", native_variable_key(item)].append(item)
        for parent in item.parent_facts:
            if parent.kind in {"register", "variant"}:
                kind = "register" if parent.kind == "register" else "register_variant"
                grouped[kind, native_parent_key(item.source, "scb", parent)].append(
                    item
                )
    result = []
    for (kind, key), members in grouped.items():
        first = members[0]
        member_key = str(key[-1])
        register_key = source_register_key(first)
        assert register_key is not None
        register_id = str(register_key[-1])
        result.append(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind=kind,
                    provider="scb",
                    source_key=key,
                    register_key=source_register_key(first)
                    if kind != "register"
                    else None,
                ),
                naming=SlugEntry(
                    kind=kind,
                    provider="scb",
                    source_id=register_id
                    if kind == "register"
                    else f"{register_id}.{member_key}",
                    slug={
                        "register": "example",
                        "register_variant": f"people-{member_key}",
                        "variable": f"value-{member_key}",
                    }[kind],
                ),
                contributors=(),
            )
        )
    return tuple(result)


def guard(item: SourceRecord):
    return PeerGuard(
        guard_id="exact-original-variable",
        source=item.source,
        coordinates=(
            ("register", item.subject.register_name),
            ("variable", item.subject.variable),
        ),
        expected_members=(record_ref(item),),
    )


def resolve(
    records,
    *,
    cases=(),
    naming=None,
    naming_ambiguities=(),
    provider_keys=None,
    derive_native_provider_keys=False,
    on_diagnostic=None,
    value_sessions=(),
    diagnostic=False,
    classifications=None,
    label_rules=None,
    classification_overrides=None,
):
    support = SourceSupportBindings((), ())
    for item in records:
        support.observe(item)
    support.seal()
    return resolve_source_scope(
        records,
        cases=cases,
        naming=names(records) if naming is None else naming,
        naming_ambiguities=naming_ambiguities,
        provider_keys={
            key: str(r.subject.variable.native_id)
            for r in records
            if (key := native_variable_key(r)) is not None
        }
        if provider_keys is None
        else provider_keys,
        derive_native_provider_keys=derive_native_provider_keys,
        value_sessions=value_sessions,
        support=support,
        classifications=classifications or {},
        classification_references={},
        label_rules=label_rules or {},
        classification_overrides=classification_overrides or {},
        on_diagnostic=on_diagnostic,
        diagnostic=diagnostic,
    )


def ambiguity(records, *, columns=None):
    first = records[0]
    expectations = capture_expectations(records, fields=("column_name",))
    return NamingAmbiguity(
        family=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=native_variable_key(first),
            register_key=source_register_key(first),
            expectations=expectations,
            peer_guards=(
                guard(first).model_copy(
                    update={"expected_members": tuple(e.ref for e in expectations)}
                ),
            ),
        ),
        entries=(
            AcceptedNamingEntry(
                revision="curation/registers/scb/example.toml",
                origin="authored",
                entry=SlugEntry("variable", "1.5.code", "code", provider="scb"),
                supplied_fields=("slug",),
                content_sha256=canonical_sha256({"slug": "code"}),
            ),
        ),
        candidate_columns=tuple(
            ("1.5.code", column)
            for column in sorted(
                columns
                if columns is not None
                else {r.fields.column_name.value for r in records}
            )
        ),
        reason="Two literal spellings share the accepted discriminator; ownership is unresolved.",
    )


def test_ambiguous_names_attribute_only_actual_unresolved_identity():
    records = (record(column="CODE"), record(2, column="Code"))
    selected = tuple(n for n in names(records) if n.target.kind != "variable")
    kwargs = {
        "naming": selected,
        "provider_keys": {native_variable_key(records[0]): None},
    }
    original = resolve(records, **kwargs)
    result = resolve(records, naming_ambiguities=(ambiguity(records),), **kwargs)
    assert result.variables == original.variables
    assert result.corrections == original.corrections
    assert result.evaluations == original.evaluations
    assert result.error_count == original.error_count + 1
    assert result.warning_count == original.warning_count == 0
    (key,) = result.withheld_dependencies
    assert key == ("variable", "scb/example/code")
    (cause,) = result.withheld_dependencies[key]
    assert cause.code == "ambiguous_named_identity"
    assert set(cause.refs) == {record_ref(r) for r in records}
    dependencies = CatalogDependencies(set(), result.withheld_dependencies)
    assert not dependencies.require(key, output="existing relation")
    dependencies.check()
    assert not dependencies.require(
        ("variable", "scb/example/unrelated"), output="typo"
    )
    with pytest.raises(CatalogDependencyError):
        dependencies.check()


def test_partial_name_keeps_supported_facts_and_only_names_observed_omissions():
    first, second = record(column="CODE"), record(2, variant=3, column="Code")
    unrelated = record(3, variant=4, column="OTHER")
    records = (first, second, unrelated)
    pending = ambiguity(records, columns=("CODE", "Code"))
    native = native_variable_key(first)
    partition = (*native, "accepted-code")
    case = CurationCase(
        case_id="existing-variant-ownership",
        targets=capture_expectations((first,), fields=("column_name",)),
        support=capture_expectations((second, unrelated), fields=("column_name",)),
        peer_guards=pending.family.peer_guards,
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            reason="Existing ownership in variant 2 only.",
            provenance="fixture",
            effects=(
                CheckedIdentityChange(ref=record_ref(first), variable_key=partition),
            ),
        ),
    )
    selected = tuple(n for n in names(records) if n.target.kind != "variable") + (
        NamingDeclaration(
            target=pending.family.model_copy(update={"source_key": partition}),
            naming=pending.names[0],
            contributors=pending.entries,
        ),
    )
    result = resolve(
        records,
        cases=(case,),
        naming=selected,
        naming_ambiguities=(pending,),
        provider_keys={native: None, partition: "5.code"},
    )
    variable = result.variables[partition]
    assert variable is not None and len(variable.states) == 1
    available = variable_dependency_keys(variable)
    assert not available & result.withheld_dependencies.keys()
    assert set(result.withheld_dependencies) == {
        ("variant_states", "scb/example/code", "people-3"),
        ("representation", "scb/example/code", "Code"),
        ("succession_representation", "scb/example/code", "code", "people-3"),
    }
    assert all(
        c.refs == (record_ref(second),)
        for causes in result.withheld_dependencies.values()
        for c in causes
    )
    dependencies = CatalogDependencies(available, result.withheld_dependencies)
    assert dependencies.require(("variable", "scb/example/code"), output="tag")
    assert not dependencies.require(
        ("variant_states", "scb/example/code", "people-4"), output="unobserved variant"
    )
    with pytest.raises(CatalogDependencyError):
        dependencies.check()


@pytest.mark.parametrize("change", ["column", "new_peer", "wrong_family"])
def test_ambiguous_naming_bridge_requires_its_whole_original_family(change):
    original = record(column="CODE")
    pending = ambiguity((original,))
    records = (original,)
    if change == "column":
        records = (
            original.model_copy(
                update={
                    "fields": original.fields.model_copy(
                        update={"column_name": value_field("NEW")}
                    )
                }
            ),
        )
    elif change == "new_peer":
        records += (record(2, column="Code"),)
    else:
        pending = pending.model_copy(
            update={
                "family": pending.family.model_copy(
                    update={"source_key": (*pending.family.source_key[:-1], 6)}
                )
            }
        )
    with pytest.raises(
        ValueError,
        match="ambiguous naming bridge is stale or belongs to another family",
    ):
        resolve(
            records,
            naming_ambiguities=(pending,),
            provider_keys={native_variable_key(original): None},
        )


def test_ambiguous_naming_cannot_hide_missing_conversion_or_known_identity():
    item = record()
    pending = ambiguity((item,))
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve((item,), naming_ambiguities=(pending,), provider_keys={})
    with pytest.raises(ValueError, match="lacks an unresolved native identity"):
        resolve((item,), naming_ambiguities=(pending,))


def test_conflicting_identity_decisions_keep_their_named_error_dependencies():
    item = record()
    pending = ambiguity((item,))
    cases = tuple(
        CurationCase(
            case_id=f"identity-{suffix}",
            targets=pending.family.expectations,
            peer_guards=pending.family.peer_guards,
            decision=OccurrenceCorrectionDecision(
                reviewed=True,
                reason="Existing conflicting assignment",
                provenance="fixture",
                effects=(
                    CheckedIdentityChange(
                        ref=record_ref(item), variable_key=("assigned", suffix)
                    ),
                ),
            ),
        )
        for suffix in ("a", "b")
    )
    result = resolve((item,), cases=cases, naming_ambiguities=(pending,))
    assert all(e.status == "applicable" for e in result.evaluations)
    assert not result.variables
    assert result.corrections.occurrences[0].withheld_fields == ("identity",)
    assert ("variable", "scb/example/code") in result.withheld_dependencies
    assert all(d.severity == "error" for d in result.diagnostics)


def test_ambiguous_names_preserve_authored_precedence_and_reject_malformed_evidence():
    pending = ambiguity((record(),))
    generated = pending.entries[0].model_copy(
        update={
            "origin": "generated",
            "entry": replace(pending.entries[0].entry, slug="old-generated"),
        }
    )
    composed = NamingAmbiguity(
        family=pending.family,
        candidate_columns=pending.candidate_columns,
        entries=(generated, *pending.entries),
        reason=pending.reason,
    )
    assert composed.names == pending.names
    with pytest.raises(ValueError, match="duplicate ambiguous naming origin"):
        NamingAmbiguity(
            family=pending.family,
            candidate_columns=pending.candidate_columns,
            entries=(*pending.entries, *pending.entries),
            reason=pending.reason,
        )
    with pytest.raises(ValueError, match="namespace"):
        NamingAmbiguity(
            family=pending.family,
            candidate_columns=pending.candidate_columns,
            entries=(
                pending.entries[0].model_copy(
                    update={
                        "entry": replace(pending.entries[0].entry, provider="other")
                    }
                ),
            ),
            reason=pending.reason,
        )


@pytest.mark.parametrize(
    "columns", [(), (("1.5.code", "absent"),), (("1.5.other", "VALUE"),)]
)
def test_ambiguous_name_candidates_must_match_names_and_original_columns(columns):
    pending = ambiguity((record(),))
    if not columns:
        assert (
            NamingAmbiguity(
                family=pending.family,
                entries=pending.entries,
                candidate_columns=(),
                reason=pending.reason,
            ).candidate_columns
            == ()
        )
        return
    with pytest.raises(ValueError, match="candidate"):
        NamingAmbiguity(
            family=pending.family,
            entries=pending.entries,
            candidate_columns=columns,
            reason=pending.reason,
        )


def test_ordinary_scope_forms_variables_with_literal_provider_keys():
    first, second = record(), record(2, variable=6, column="OTHER")
    result = resolve((first, second))
    assert result.evaluations == () and result.diagnostics == ()
    assert len(result.parents.registers) == len(result.parents.variants) == 1
    assert {v.provider_key for v in result.variables.values()} == {"5", "6"}
    assert sum(len(v.states) for v in result.variables.values()) == 2
    assert len(result.corrections.occurrences) == 2


def test_scope_routes_checked_delivery_units_to_literal_state_formation():
    records = tuple(
        item.model_copy(
            update={
                "fields": item.fields.model_copy(
                    update={
                        "definition": value_field("Annual received amount"),
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
        case_id="checked-literal-delivery-units",
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
        decision=DeliveryUnitDecision(
            reviewed=True,
            variable_key=native_variable_key(records[0]),
            columns=(
                DeliveryUnitColumn(
                    variant_key=native_variant_key(records[0]),
                    column="VALUE",
                    valid_from="2020-01-01",
                    valid_to="2021-12-31",
                    expected_codings=(),
                ),
            ),
            reason="Retain each supplied unit without converting amounts.",
            provenance="exact source fixture",
        ),
    )
    result = resolve(records, cases=(case,))
    assert [(issue.code, issue.severity) for issue in result.diagnostics] == [
        ("delivery_unit_projected", "warning")
    ]
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


def test_interleaved_occurrences_share_bound_lists_and_keep_every_evidence_use(
    tmp_path, monkeypatch
):
    first, second = record(1), record(2)
    root = tmp_path / "values"
    manifest = prepare_source_values(
        root,
        revision=REVISION,
        validity_revision=REVISION,
        descriptors=(SourceValueDescriptor("list"),),
        values=(SourceValue("value", "01", "One"),),
        associations=tuple(
            SourceValueAssociation(
                i + 1, "list", "value", "values", member_id=str(i), item_id="1"
            )
            for i in (1, 2)
        ),
        join=SourceValueJoin(
            record_sources=(REVISION.dataset,),
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
    lookups = []
    with open_value_bindings((source,)) as sessions:
        session = sessions[0].session
        original = session.lookup_native_member

        def lookup(member):
            lookups.append(member)
            return original(member)

        monkeypatch.setattr(session, "lookup_native_member", lookup)
        result = resolve((first, second, first, second), value_sessions=sessions)
    assert lookups == [1, 2]
    assert not result.diagnostics
    assert len(result.corrections.occurrences) == 4
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    assert [
        (s.valid_from, s.valid_to, s.value_set.members) for s in variable.states
    ] == [("2020-01-01", "2020-12-31", (("01", "One"),))]


def test_whole_list_item_validity_override_emits_one_warning(tmp_path):
    item = record(year="1990")
    root = tmp_path / "values"
    rows = (
        SourceValueAssociation(2, "list", "no", "values", member_id="1", item_id="1"),
        SourceValueAssociation(3, "list", "yes", "values", member_id="1", item_id="2"),
    )
    manifest = prepare_source_values(
        root,
        revision=REVISION,
        validity_revision=REVISION,
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
            record_sources=(REVISION.dataset,),
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
        revision=REVISION,
        validity_revision=REVISION,
        descriptors=(SourceValueDescriptor("list", version=" Listed "),),
        values=(SourceValue("value", "01", "One"),),
        associations=(
            SourceValueAssociation(
                1, "list", "value", "values", member_id="1", item_id="1"
            ),
        ),
        join=SourceValueJoin(
            record_sources=(REVISION.dataset,),
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
        ruled.variables[native_variable_key(item)].states[0].classification == "first"
    )
    assert (
        overridden.variables[native_variable_key(item)].states[0].classification
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
        revision=REVISION,
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
            record_sources=(REVISION.dataset,),
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
            revision=REVISION,
            validity_revision=REVISION,
            descriptors=(SourceValueDescriptor("list"),),
            values=(
                SourceValue("code", code, "Original label"),
                SourceValue("future", "99", "Future"),
            ),
            associations=rows,
            join=SourceValueJoin(
                record_sources=(REVISION.dataset,),
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


def test_checked_naming_retains_accepted_deprecation():
    item = record()
    selected = tuple(
        n.model_copy(update={"naming": replace(n.naming, deprecated=True)})
        if n.target.kind == "variable"
        else n
        for n in names((item,))
    )
    result = resolve((item,), naming=selected)
    variable = result.variables[native_variable_key(item)]
    assert variable is not None and variable.deprecated


def test_coding_is_checked_despite_unresolved_catalog_identity():
    item = record()
    column_key = native_column_key(item)
    assert column_key is not None
    case = CurationCase(
        case_id="accepted-uncoded",
        targets=capture_expectations((item,), fields=("column_name",)),
        peer_guards=(guard(item),),
        decision=CodingDecision(
            reviewed=True,
            column_key=column_key,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            expected_codings=(),
            selection="uncoded",
            reason="Accepted uncoded period",
            provenance="Fixture decision",
        ),
    )
    result = resolve(
        (item,), cases=(case,), provider_keys={native_variable_key(item): None}
    )
    assert [(e.case_id, e.status) for e in result.evaluations] == [
        (case.case_id, "applicable")
    ]
    assert result.variables == {native_variable_key(item): None}
    assert [d.code for d in result.diagnostics] == ["unresolved_catalog_identity"]


def test_columnless_only_remainder_warns_with_original_refs():
    items = tuple(
        item.model_copy(
            update={
                "fields": item.fields.model_copy(
                    update={"column_name": SourceField(status="negative", raw_value="")}
                )
            }
        )
        for item in (record(1), record(2, year="2021"))
    )
    key = native_variable_key(items[0])
    naming = tuple(name for name in names(items) if name.target.kind != "variable")
    result = resolve(items, naming=naming, provider_keys={key: None})
    assert result.variables[key] is None
    assert [(issue.code, issue.severity) for issue in result.diagnostics] == [
        ("omitted_columnless_occurrence", "warning")
    ]
    assert set(result.diagnostics[0].refs) == {record_ref(item) for item in items}


def test_unknown_column_keeps_remainder_identity_error():
    first = record()
    second = record(2, year="2021")
    columnless = first.model_copy(
        update={
            "fields": first.fields.model_copy(
                update={"column_name": SourceField(status="negative", raw_value="")}
            )
        }
    )
    unknown = second.model_copy(
        update={
            "fields": second.fields.model_copy(
                update={"column_name": SourceField(status="unknown")}
            )
        }
    )
    key = native_variable_key(columnless)
    naming = tuple(
        name for name in names((columnless, unknown)) if name.target.kind != "variable"
    )
    result = resolve((columnless, unknown), naming=naming, provider_keys={key: None})
    assert [(issue.code, issue.severity) for issue in result.diagnostics] == [
        ("unresolved_catalog_identity", "error")
    ]


def test_withheld_naming_keeps_columnless_identity_error():
    original = record()
    columnless = original.model_copy(
        update={
            "fields": original.fields.model_copy(
                update={"column_name": SourceField(status="negative", raw_value="")}
            )
        }
    )
    key = native_variable_key(columnless)
    naming = tuple(
        name.model_copy(
            update={
                "target": name.target.model_copy(
                    update={
                        "expectations": capture_expectations(
                            (original,), fields=("column_name",)
                        ),
                        "peer_guards": (guard(original),),
                    }
                )
            }
        )
        if name.target.kind == "variable"
        else name
        for name in names((original,))
    )
    result = resolve((columnless,), naming=naming, provider_keys={key: None})
    assert any(
        issue.code == "unresolved_catalog_identity" for issue in result.diagnostics
    )
    assert not any(
        issue.code == "omitted_columnless_occurrence" for issue in result.diagnostics
    )


@pytest.mark.parametrize("register_id", [1, 258])
def test_native_coding_column_key_follows_checked_partition(register_id: int):
    item = record(column="VALUE", register_id=register_id)
    native = native_variable_key(item)
    column = native_column_key(item)
    assert native is not None and column is not None
    partition = convert_column_partitions(
        (item,),
        source_id=f"{register_id}.5",
        split_ids=(f"{register_id}.5.value",),
    )
    assert partition.case is not None
    split = partition.bindings[0].target.source_key
    coding = CurationCase(
        case_id="accepted-uncoded-base-key",
        targets=capture_expectations((item,), fields=("column_name",)),
        peer_guards=(guard(item),),
        decision=CodingDecision(
            reviewed=True,
            column_key=column,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            expected_codings=(),
            selection="uncoded",
            reason="Accepted uncoded period",
            provenance="Fixture decision",
        ),
    )
    split_name = NamingDeclaration(
        target=partition.bindings[0].target,
        naming=SlugEntry(
            kind="variable",
            provider="scb",
            source_id=f"{register_id}.5.value",
            slug="value",
        ),
        contributors=(),
    )
    naming = tuple(
        item for item in names((item,)) if item.target.kind != "variable"
    ) + (split_name,)

    result = resolve(
        (item,),
        cases=(partition.case, coding),
        naming=naming,
        provider_keys={split: "5.value"},
    )
    assert {item.case_id: item.status for item in result.evaluations} == {
        partition.case.case_id: "applicable",
        coding.case_id: "applicable",
    }
    assert split in result.variables


def test_native_classification_column_key_follows_checked_partition():
    item = record(column="VALUE")
    native = native_variable_key(item)
    column = native_column_key(item)
    assert native is not None and column is not None
    partition = convert_column_partitions(
        (item,), source_id="1.5", split_ids=("1.5.value",)
    )
    assert partition.case is not None
    split = partition.bindings[0].target.source_key
    book = ResolvedClassification(
        slug="fixture",
        short_name="FIXTURE",
        name="Fixture",
        codes=(ResolvedClassificationCode(code="01", label="One"),),
    )
    classification = CurationCase(
        case_id="accepted-classification-base-key",
        targets=capture_expectations((item,), fields=("column_name",), coding=True),
        peer_guards=(guard(item),),
        decision=ClassificationDecision(
            reviewed=True,
            column_key=column,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            expected_codings=(),
            classification="fixture",
            expected_classification="0" * 64,
            binding_scope="declared",
            reason="Accepted fixture classification",
            provenance="Fixture decision",
        ),
    )
    split_name = NamingDeclaration(
        target=partition.bindings[0].target,
        naming=SlugEntry(
            kind="variable", provider="scb", source_id="1.5.value", slug="value"
        ),
        contributors=(),
    )
    naming = tuple(
        item for item in names((item,)) if item.target.kind != "variable"
    ) + (split_name,)
    result = resolve(
        (item,),
        cases=(partition.case, classification),
        naming=naming,
        provider_keys={split: "5.value"},
        classifications={"fixture": book},
    )
    assert {item.case_id: item.status for item in result.evaluations} == {
        partition.case.case_id: "applicable",
        classification.case_id: "applicable",
    }
    assert split in result.variables


def test_stale_partition_case_withholds_unsplit_family_in_diagnostic_mode():
    first, second = record(column="VALUE"), record(2, column="OTHER")
    native = native_variable_key(first)
    assert native is not None and native_variable_key(second) == native
    split = ((*native, "accepted-partition-i1"), (*native, "accepted-partition-i2"))
    case = CurationCase(
        case_id="accepted-column-partitions",
        targets=capture_expectations((first, second), fields=("column_name",)),
        peer_guards=(
            guard(first).model_copy(
                update={"expected_members": (record_ref(first), record_ref(second))}
            ),
        ),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            reason="Accepted column partitions.",
            provenance="fixture",
            effects=(
                CheckedIdentityChange(ref=record_ref(first), variable_key=split[0]),
                CheckedIdentityChange(ref=record_ref(second), variable_key=split[1]),
            ),
        ),
    )
    stale = tuple(
        item.model_copy(
            update={
                "fields": item.fields.model_copy(
                    update={"column_name": value_field(f"STALE-{index}")}
                )
            }
        )
        for index, item in enumerate((first, second))
    )
    provider_keys = {split[0]: "5.i1", split[1]: "5.i2"}
    result = resolve(stale, cases=(case,), provider_keys=provider_keys, diagnostic=True)
    assert [e.status for e in result.evaluations] == ["stale"]
    assert {d.code for d in result.diagnostics} == {
        "target_projection_changed",
        "unresolved_catalog_identity",
    }
    assert result.variables == {native: None}
    assert set(result.withheld_dependencies) == {("variable", "scb/example/value-5")}
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve(stale, cases=(case,), provider_keys=provider_keys)
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve(stale, provider_keys=provider_keys, diagnostic=True)
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve(
            stale,
            cases=(case,),
            provider_keys={split[0]: "5.i1"},
            diagnostic=True,
        )
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve(
            stale,
            cases=(case,),
            provider_keys={split[0]: "5.i1", (*native, "other"): "5.x"},
            diagnostic=True,
        )


def test_alias_window_checks_competing_variables_in_the_whole_scope():
    first, second = record(), record(2, variable=6)
    variable_key, variant_key = native_variable_key(first), native_variant_key(first)
    assert variable_key is not None and variant_key is not None
    case = CurationCase(
        case_id="accepted-window",
        targets=capture_expectations((first,), fields=("column_name",)),
        peer_guards=(guard(first),),
        decision=AliasWindowDecision(
            reviewed=True,
            variable_key=variable_key,
            variant_key=variant_key,
            column="VALUE",
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            reason="Existing bounded representation",
            provenance="Fixture decision",
        ),
    )
    result = resolve((first, second), cases=(case,))
    assert [e.status for e in result.evaluations] == ["applicable"]
    assert [d.code for d in result.diagnostics] == ["conflicting_alias_window_owner"]
    assert all(not v.aliases for v in result.variables.values())


def test_withheld_variant_keeps_supported_sibling_states():
    first, second = record(), record(2, variant=3)
    changed_parents = tuple(
        p.model_copy(update={"fields": p.fields.model_copy(update={"name": None})})
        if p.kind == "variant"
        else p
        for p in second.parent_facts
    )
    second = second.model_copy(update={"parent_facts": changed_parents})
    result = resolve((first, second))
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    assert [s.variant.slug for s in variable.states] == ["people-2"]
    problem = next(
        d for d in result.diagnostics if d.code == "withheld_variant_dependency"
    )
    assert problem.refs == (record_ref(second),) and problem.withheld_output == (
        "state",
    )


def test_missing_conversion_is_fatal():
    item = record()
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve((item,), provider_keys={})
    with pytest.raises(ValueError, match="missing checked parent naming"):
        resolve((item,), naming=())


def test_compiled_native_provider_key_comes_from_final_naming():
    item = record()
    key = native_variable_key(item)
    named = resolve((item,), provider_keys={}, derive_native_provider_keys=True)
    assert named.variables[key] is not None
    assert named.variables[key].provider_key == "5"
    unnamed = resolve(
        (item,),
        naming=tuple(name for name in names((item,)) if name.target.kind != "variable"),
        provider_keys={},
        derive_native_provider_keys=True,
    )
    assert unnamed.variables[key] is None
    assert any(
        issue.code == "unresolved_catalog_identity" for issue in unnamed.diagnostics
    )


def test_stored_period_family_name_keeps_its_provider_key_beside_native_names():
    item = record()
    native_key = native_variable_key(item)
    period_key = ("curation", "period-family", "scb", 1, "value")
    period_name = NamingDeclaration(
        target=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=period_key,
            register_key=source_register_key(item),
        ),
        naming=SlugEntry(
            kind="variable",
            provider="scb",
            source_id="accepted-period-family:5",
            slug="period-value",
        ),
        contributors=(),
    )
    result = resolve(
        (item,),
        naming=(*names((item,)), period_name),
        provider_keys={period_key: "period-value"},
        derive_native_provider_keys=True,
    )
    assert result.variables[native_key] is not None
    assert result.variables[native_key].provider_key == "5"


def test_native_provider_key_rejects_malformed_source_id_with_target():
    item = record()
    native_key = native_variable_key(item)
    naming = tuple(
        declaration.model_copy(
            update={
                "naming": replace(
                    declaration.naming, source_id="accepted-period-family:5"
                )
            }
        )
        if declaration.target.source_key == native_key
        else declaration
        for declaration in names((item,))
    )
    with pytest.raises(ValueError, match="requires an R.V source_id") as error:
        resolve(
            (item,),
            naming=naming,
            provider_keys={},
            derive_native_provider_keys=True,
        )
    assert repr(native_key) in str(error.value)


def test_streamed_diagnostics_preserve_strict_severity_and_source_refs():
    item = record()
    key = native_variable_key(item)
    collected = resolve((item,), provider_keys={key: None})
    emitted = []
    streamed = resolve((item,), provider_keys={key: None}, on_diagnostic=emitted.append)
    assert tuple(emitted) == collected.diagnostics
    assert streamed.diagnostics == ()
    assert streamed.error_count == collected.error_count == 1
    assert streamed.warning_count == collected.warning_count == 0
    assert emitted[0].refs == (record_ref(item),)
    assert streamed.withheld_dependencies == collected.withheld_dependencies


def acknowledge(issue, item):
    register_key = source_register_key(item)
    assert register_key is not None
    return CurationCase(
        case_id="acknowledged",
        targets=(),
        decision=AcknowledgeDecision(
            code=issue.code,
            subject=issue.subject,
            refs=issue.refs,
            fields=issue.fields,
            valid_from=issue.valid_from,
            valid_to=issue.valid_to,
            register_key=register_key,
            reason="Accepted while the source stays unresolved.",
            evidence="Fixture diagnostic ledger.",
        ),
    )


def test_an_acknowledged_error_becomes_a_counted_warning_and_stays_withheld():
    item = record()
    key = native_variable_key(item)
    (issue,) = resolve((item,), provider_keys={key: None}).diagnostics
    case = acknowledge(issue, item)
    result = resolve((item,), cases=(case,), provider_keys={key: None})
    warning = issue.model_copy(
        update={"severity": "warning", "acknowledged_by": "acknowledged"}
    )
    assert result.diagnostics == (warning,)
    assert (result.error_count, result.warning_count) == (0, 1)
    assert result.acknowledged == {"unresolved_catalog_identity": 1}
    assert result.variables == {key: None}
    assert result.withheld_dependencies == {
        ("variable", "scb/example/value-5"): (warning,)
    }
    assert [(e.case_id, e.status) for e in result.evaluations] == [
        ("acknowledged", "applicable")
    ]
    with pytest.raises(ValueError, match="acknowledged twice"):
        resolve(
            (item,),
            cases=(case, case.model_copy(update={"case_id": "again"})),
            provider_keys={key: None},
        )


def test_a_stale_acknowledgement_is_an_error():
    item = record()
    key = native_variable_key(item)
    (issue,) = resolve((item,), provider_keys={key: None}).diagnostics
    # The issue no longer occurs: the variable forms and the acknowledgement errs.
    result = resolve((item,), cases=(acknowledge(issue, item),))
    assert result.variables[key] is not None
    (stale,) = result.diagnostics
    assert (stale.code, stale.severity, stale.case_id) == (
        "stale_curation_entry",
        "error",
        "acknowledged",
    )
    assert result.acknowledged == {}
    # Coordinates match exactly, never by containment.
    other = record(2, year="2021")
    result = resolve(
        (item, other),
        cases=(acknowledge(issue, item),),
        provider_keys={key: None},
    )
    assert [(d.code, d.severity) for d in result.diagnostics] == [
        ("unresolved_catalog_identity", "error"),
        ("stale_curation_entry", "error"),
    ]


def test_distinct_field_issues_can_each_be_acknowledged():
    items = tuple(
        item.model_copy(
            update={
                "fields": item.fields.model_copy(
                    update={
                        "definition": value_field(text),
                        "description": value_field(text),
                    }
                )
            }
        )
        for item, text in ((record(), "first"), (record(2, year="2021"), "second"))
    )
    conflicts = [
        d for d in resolve(items).diagnostics if d.code == "conflicting_variable_fact"
    ]
    assert len(conflicts) == 2
    assert len({(d.subject, d.refs) for d in conflicts}) == 1
    assert len({d.fields for d in conflicts}) == 2
    cases = tuple(
        acknowledge(issue, items[0]).model_copy(update={"case_id": f"ack-{index}"})
        for index, issue in enumerate(conflicts, 1)
    )
    result = resolve(items, cases=cases)
    warnings = tuple(
        d for d in result.diagnostics if d.code == "conflicting_variable_fact"
    )
    assert warnings == tuple(
        issue.model_copy(
            update={"severity": "warning", "acknowledged_by": f"ack-{index}"}
        )
        for index, issue in enumerate(conflicts, 1)
    )
    assert result.acknowledged == {"conflicting_variable_fact": 2}
    no_fields = acknowledge(conflicts[0], items[0])
    no_fields = no_fields.model_copy(
        update={"decision": no_fields.decision.model_copy(update={"fields": ()})}
    )
    stale = resolve(items, cases=(no_fields,))
    assert [d.code for d in stale.diagnostics] == [
        "conflicting_variable_fact",
        "conflicting_variable_fact",
        "stale_curation_entry",
    ]
    assert stale.acknowledged == {}


def test_period_key_is_exact_and_stale_without_dates(monkeypatch):
    item = record()
    ref = record_ref(item)
    first = ResolutionDiagnostic(
        code="period_issue",
        severity="error",
        subject="scb/example/value-5",
        detail="First window",
        refs=(ref,),
        valid_from="2020-01-01",
        valid_to="2020-06-30",
    )
    second = first.model_copy(
        update={
            "detail": "Second window",
            "valid_from": "2020-07-01",
            "valid_to": "2020-12-31",
        }
    )
    monkeypatch.setattr(
        "reg_meta_build.source_scope.resolve_sibling_pairs",
        lambda *_args, **_kwargs: SiblingResolution((), (), (first, second)),
    )
    cases = tuple(
        acknowledge(issue, item).model_copy(update={"case_id": f"ack-{index}"})
        for index, issue in enumerate((first, second), 1)
    )
    result = resolve((item,), cases=cases)
    assert result.diagnostics == tuple(
        issue.model_copy(
            update={"severity": "warning", "acknowledged_by": f"ack-{index}"}
        )
        for index, issue in enumerate((first, second), 1)
    )
    assert result.acknowledged == {"period_issue": 2}

    no_period = acknowledge(first, item).model_copy(
        update={
            "decision": acknowledge(first, item).decision.model_copy(
                update={"valid_from": None, "valid_to": None}
            )
        }
    )
    stale = resolve((item,), cases=(no_period,))
    assert [d.code for d in stale.diagnostics] == [
        "period_issue",
        "period_issue",
        "stale_curation_entry",
    ]
    assert stale.acknowledged == {}


def test_acknowledgement_field_order_is_exact(monkeypatch):
    item = record()
    issue = ResolutionDiagnostic(
        code="ordered_fields_issue",
        severity="error",
        subject="scb/example/value-5",
        detail="Ordered diagnostic fields",
        refs=(record_ref(item),),
        fields=("name", "description"),
    )
    monkeypatch.setattr(
        "reg_meta_build.source_scope.resolve_sibling_pairs",
        lambda *_args, **_kwargs: SiblingResolution((), (), (issue,)),
    )
    case = acknowledge(issue, item)
    reversed_case = case.model_copy(
        update={
            "decision": case.decision.model_copy(
                update={"fields": ("description", "name")}
            )
        }
    )
    result = resolve((item,), cases=(reversed_case,))
    assert result.diagnostics[0] == issue
    assert result.diagnostics[1].code == "stale_curation_entry"


def test_an_overbroad_acknowledgement_is_an_error_and_acknowledges_nothing(monkeypatch):
    item = record()
    issue = ResolutionDiagnostic(
        code="repeated_issue",
        severity="error",
        subject="scb/example/value-5",
        detail="Two occurrences with one identity",
        refs=(record_ref(item),),
    )
    monkeypatch.setattr(
        "reg_meta_build.source_scope.resolve_sibling_pairs",
        lambda *_args, **_kwargs: SiblingResolution((), (), (issue, issue)),
    )
    result = resolve((item,), cases=(acknowledge(issue, item),))
    assert [d.severity for d in result.diagnostics if d.code == "repeated_issue"] == [
        "error",
        "error",
    ]
    (overbroad,) = (
        d for d in result.diagnostics if d.code == "overbroad_curation_entry"
    )
    assert (overbroad.severity, overbroad.case_id) == ("error", "acknowledged")
    assert result.acknowledged == {}


def test_pooled_variant_dependency_withholds_only_its_panel_axis():
    first, second = record(), record(2, variant=3)
    pooled = TemporalScope(kind="pooled", label="2019-2020")
    second = second.model_copy(
        update={"edition_scope": pooled, "edition_period_scope": pooled}
    )
    selected = tuple(
        n.model_copy(
            update={
                "naming": replace(
                    n.naming, panel_entity_key="value-5", panel_time_key="period"
                )
            }
        )
        if n.target.kind == "register_variant"
        else n
        for n in names((first, second))
    )
    result = resolve((first, second), naming=selected)
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    omitted = ("variant_states", "scb/example/value-5", "people-3")
    assert set(result.withheld_dependencies) == {omitted}
    causes = result.withheld_dependencies[omitted]
    assert all(
        c.code == "unsupported_occurrence" and c.refs == (record_ref(second),)
        for c in causes
    )
    register = next(iter(result.parents.registers.values()))
    panel = resolve_panel_dependencies(
        (variable,),
        registers=(register,),
        variants=tuple((register, v) for v in result.parents.variants.values()),
        withheld=result.withheld_dependencies,
    )
    variants = {v.slug: v for _, v in panel.variants}
    assert variants["people-2"].panel_entity_key == "value-5"
    assert variants["people-3"].panel_entity_key is None
    assert variants["people-3"].panel_time_key == "period"


def test_parallel_columns_report_exact_omissions_without_hiding_a_safe_variant():
    first, second, third = (
        record(column="VALUE"),
        record(2, column="OTHER"),
        record(3, variant=3),
    )
    records = (first, second, third)
    key = native_variable_key(first)
    assert key is not None
    case = CurationCase(
        case_id="accepted-shared-identity",
        targets=capture_expectations(records, fields=("column_name",)),
        peer_guards=(
            guard(first).model_copy(
                update={"expected_members": tuple(record_ref(r) for r in records)}
            ),
        ),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(
                CheckedIdentityChange(ref=record_ref(r), variable_key=key)
                for r in records
            ),
            reason="Existing shared identity",
            provenance="Fixture declaration",
        ),
    )
    result = resolve(records, cases=(case,))
    variable = result.variables[key]
    assert variable is not None and len(variable.states) == 1
    assert variable.states[0].variant.slug == "people-3"
    assert (
        "variant_states",
        "scb/example/value-5",
        "people-2",
    ) in result.withheld_dependencies
    assert (
        "representation",
        "scb/example/value-5",
        "OTHER",
    ) in result.withheld_dependencies
    assert (
        "representation",
        "scb/example/value-5",
        "VALUE",
    ) not in result.withheld_dependencies
    assert (
        "succession_representation",
        "scb/example/value-5",
        "value",
        "people-2",
    ) in result.withheld_dependencies
    assert (
        "succession_representation",
        "scb/example/value-5",
        "value",
        "people-3",
    ) not in result.withheld_dependencies
    causes = result.withheld_dependencies[
        "representation", "scb/example/value-5", "OTHER"
    ]
    assert all(set(c.refs) == {record_ref(first), record_ref(second)} for c in causes)
    assert all(
        c.valid_from == "2020-01-01" and c.valid_to == "2020-12-31" for c in causes
    )
    dependencies = CatalogDependencies(
        variable_dependency_keys(variable), result.withheld_dependencies
    )
    assert not dependencies.require(
        ("representation", "scb/example/value-5", "NEVER_DOCUMENTED"), output="group"
    )
    with pytest.raises(CatalogDependencyError, match="NEVER_DOCUMENTED"):
        dependencies.check()
    # The exact parallel-column blocker stays a curation blocker: it withholds
    # its own window, so only the safe variant still owes delivery.
    assert [
        (o.variant, o.column, o.valid_from, o.valid_to) for o in result.coverage
    ] == [("people-3", "VALUE", "2020-01-01", "2020-12-31")]
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    # Withholding every state of one variant is evidenced and exact: it answers for
    # that variant's own claim, and leaves the safe sibling's claim checked.
    claim = CoverageObligation(
        "scb/example/value-5",
        "people-2",
        "OTHER",
        "2020-01-01",
        "2020-12-31",
        (record_ref(second),),
    )
    check_delivery_coverage(
        (variable,), (claim,), withheld=result.withheld_dependencies
    )
    with pytest.raises(ValueError, match=r"people-3/OTHER 2020-01-01\.\.2020-12-31"):
        check_delivery_coverage(
            (variable,),
            (replace(claim, variant="people-3"),),
            withheld=result.withheld_dependencies,
        )


def _states(variable, shape):
    """Damage one whole-2020 state the way an engineering defect would."""
    (state,) = variable.states
    if shape == "half_year":
        return (state.model_copy(update={"valid_to": "2020-06-30"}),)
    if shape == "exact_day":
        return (
            state.model_copy(update={"valid_to": "2020-03-06"}),
            state.model_copy(update={"valid_from": "2020-03-08"}),
        )
    moved = state.model_copy(update={"valid_from": "2020-07-01"})
    return (
        state.model_copy(update={"valid_to": "2020-06-30"}),
        moved.model_copy(
            update={"delivery_column_name": "OTHER"}
            if shape == "column"
            else {"variant": ResolvedVariant(slug="people-3", name="Households")}
        ),
    )


@pytest.mark.parametrize(
    "shape,missing",
    [
        ("half_year", "2020-07-01..2020-12-31"),
        ("exact_day", "2020-03-07..2020-03-07"),
        ("column", "2020-07-01..2020-12-31"),
        ("variant", "2020-07-01..2020-12-31"),
    ],
)
def test_lost_delivery_coverage_is_refused_with_its_exact_window(shape, missing):
    item = record()
    result = resolve((item,))
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    assert [
        (o.fqid, o.variant, o.column, o.valid_from, o.valid_to, o.refs)
        for o in result.coverage
    ] == [
        (
            "scb/example/value-5",
            "people-2",
            "VALUE",
            "2020-01-01",
            "2020-12-31",
            (record_ref(item),),
        )
    ]
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    damaged = variable.model_copy(update={"states": _states(variable, shape)})
    with pytest.raises(ValueError, match="delivery coverage was lost") as failure:
        check_delivery_coverage(
            (damaged,), result.coverage, withheld=result.withheld_dependencies
        )
    assert missing in str(failure.value)
    assert "scb/example/value-5 people-2/VALUE" in str(failure.value)
    assert f"claimed by {REVISION.dataset}/" in str(failure.value)


@pytest.mark.parametrize("shape", ["gap", "negative", "unknown", "pooled"])
def test_genuine_source_gaps_and_unresolved_scopes_claim_no_delivery(shape):
    first, second = record(year="2019"), record(2, year="2021")
    if shape == "negative":
        second = second.model_copy(
            update={
                "fields": second.fields.model_copy(
                    update={"availability": SourceField(status="negative")}
                )
            }
        )
    elif shape == "unknown":
        second = second.model_copy(
            update={
                "edition_scope": TemporalScope(kind="unknown", label="okänd"),
                "edition_period_scope": TemporalScope(kind="unknown", label="okänd"),
            }
        )
    elif shape == "pooled":
        pooled = TemporalScope(kind="pooled", label="2021-2022")
        second = second.model_copy(
            update={"edition_scope": pooled, "edition_period_scope": pooled}
        )
    result = resolve((first, second))
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    claimed = [(o.valid_from, o.valid_to) for o in result.coverage]
    assert claimed == (
        [("2019-01-01", "2019-12-31"), ("2021-01-01", "2021-12-31")]
        if shape == "gap"
        else [("2019-01-01", "2019-12-31")]
    )
    # No obligation covers 2020, the withdrawn year, or any widened period.
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )


def test_a_checked_state_omission_withdraws_only_its_own_period():
    first, second = record(year="2019"), record(2, year="2020")
    column_key = native_column_key(second)
    assert column_key is not None
    case = CurationCase(
        case_id="accepted-omission",
        targets=capture_expectations((second,), fields=("column_name",)),
        peer_guards=(
            guard(second).model_copy(
                update={"expected_members": (record_ref(first), record_ref(second))}
            ),
        ),
        decision=CodingDecision(
            reviewed=True,
            column_key=column_key,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            expected_codings=(),
            selection="omit_state",
            reason="Accepted omitted delivery state",
            provenance="Fixture decision",
        ),
    )
    result = resolve((first, second), cases=(case,))
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    assert [d.code for d in result.diagnostics] == ["curated_state_omission"]
    assert [(s.valid_from, s.valid_to) for s in variable.states] == [
        ("2019-01-01", "2019-12-31")
    ]
    assert [(o.valid_from, o.valid_to) for o in result.coverage] == [
        ("2019-01-01", "2019-12-31")
    ]
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    lost = variable.model_copy(
        update={
            "states": (
                variable.states[0].model_copy(update={"valid_to": "2019-06-30"}),
            )
        }
    )
    with pytest.raises(ValueError, match=r"2019-07-01\.\.2019-12-31"):
        check_delivery_coverage(
            (lost,), result.coverage, withheld=result.withheld_dependencies
        )


def test_an_unrelated_field_diagnostic_is_no_permission_to_discard_its_state():
    item = record()
    item = item.model_copy(
        update={"fields": item.fields.model_copy(update={"data_type": None})}
    )
    result = resolve((item,))
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    assert [d.code for d in result.diagnostics] == ["unknown_data_type"]
    assert [(o.valid_from, o.valid_to) for o in result.coverage] == [
        ("2020-01-01", "2020-12-31")
    ]
    with pytest.raises(ValueError, match=r"2020-01-01\.\.2020-12-31"):
        check_delivery_coverage(
            (), result.coverage, withheld=result.withheld_dependencies
        )


def test_an_evidenced_whole_variable_withholding_answers_for_its_own_claim():
    item = record()
    result = resolve(
        (
            item.model_copy(
                update={
                    "fields": item.fields.model_copy(
                        update={"sensitivity": SourceField(status="unknown")}
                    )
                }
            ),
        )
    )
    assert result.variables == {native_variable_key(item): None}
    assert [d.code for d in result.diagnostics] == ["unresolved_flag"]
    # The occurrence still claims 2020. The evidenced whole-variable withholding is
    # what answers for it, so the loss stays a curation blocker, not an our-bug
    # failure — and nothing but that ledger entry excuses it.
    assert [(o.valid_from, o.valid_to) for o in result.coverage] == [
        ("2020-01-01", "2020-12-31")
    ]
    assert ("variable", "scb/example/value-5") in result.withheld_dependencies
    check_delivery_coverage((), result.coverage, withheld=result.withheld_dependencies)
    with pytest.raises(ValueError, match=r"2020-01-01\.\.2020-12-31"):
        check_delivery_coverage((), result.coverage, withheld={})


def test_a_search_only_alias_establishes_no_delivery_for_the_lost_window():
    item = record()
    variable_key, variant_key = native_variable_key(item), native_variant_key(item)
    assert variable_key is not None and variant_key is not None
    case = CurationCase(
        case_id="accepted-spelling",
        targets=capture_expectations((item,), fields=("column_name",)),
        peer_guards=(guard(item),),
        decision=SearchAliasDecision(
            reviewed=True,
            variable_key=variable_key,
            variant_keys=(variant_key,),
            column="VALUE",
            reason="Existing search spelling",
            provenance="Fixture decision",
        ),
    )
    result = resolve((item,), cases=(case,))
    variable = result.variables[variable_key]
    assert variable is not None
    assert [(a.delivery_column_name, a.windows) for a in variable.aliases] == [
        ("VALUE", ())
    ]
    damaged = variable.model_copy(
        update={
            "states": (
                variable.states[0].model_copy(update={"valid_to": "2020-06-30"}),
            )
        }
    )
    with pytest.raises(ValueError, match=r"2020-07-01\.\.2020-12-31"):
        check_delivery_coverage(
            (damaged,), result.coverage, withheld=result.withheld_dependencies
        )


def test_supported_window_facts_reach_the_written_state():
    item = record()
    result = resolve((item,))
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    (obligation,) = result.coverage
    assert (obligation.data_type_claim, obligation.data_length_claim) == (
        ("value", "integer"),
        ("value", "1"),
    )
    assert obligation.attributions == ()
    state = variable.states[0]
    assert (state.data_type, state.data_length) == ("integer", "1")
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    for field, written in (("data_type", "text"), ("data_length", "0")):
        damaged = variable.model_copy(
            update={"states": (state.model_copy(update={field: written}),)}
        )
        with pytest.raises(
            ValueError,
            match="supported delivery facts changed without an explicit source outcome",
        ) as failure:
            check_delivery_coverage(
                (damaged,), result.coverage, withheld=result.withheld_dependencies
            )
        assert f"claimed {field}=" in str(failure.value)
        assert "scb/example/value-5 people-2/VALUE 2020-01-01..2020-12-31" in str(
            failure.value
        )


def test_copied_window_length_is_refused_as_a_changed_fact():
    first = record(year="2019")
    second = record(2, year="2021")
    second = second.model_copy(
        update={
            "fields": second.fields.model_copy(update={"data_length": value_field("2")})
        }
    )
    result = resolve((first, second))
    variable = result.variables[native_variable_key(first)]
    assert variable is not None
    assert [(o.valid_from, o.data_length_claim) for o in result.coverage] == [
        ("2019-01-01", ("value", "1")),
        ("2021-01-01", ("value", "2")),
    ]
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    copied = tuple(
        s.model_copy(update={"data_length": "1"}) if s.valid_from >= "2021" else s
        for s in variable.states
    )
    damaged = variable.model_copy(update={"states": copied})
    with pytest.raises(
        ValueError, match="claimed data_length='2' written '1'"
    ) as failure:
        check_delivery_coverage(
            (damaged,), result.coverage, withheld=result.withheld_dependencies
        )
    assert "supported delivery facts changed without an explicit source outcome" in str(
        failure.value
    )


def test_member_correction_attributions_reach_the_written_state():
    item = record()
    case = CurationCase(
        case_id="fix-opdef",
        targets=capture_expectations((item,), fields=("operational_definition",)),
        peer_guards=(guard(item),),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            reason="Checked attribution fix",
            provenance="fixture:attr",
            effects=(
                CheckedFieldChange(
                    ref=record_ref(item),
                    replacement=FieldExpectation(
                        name="operational_definition", status="value", value="Fixed"
                    ),
                ),
            ),
        ),
    )
    result = resolve((item,), cases=(case,))
    variable = result.variables[native_variable_key(item)]
    assert variable is not None
    (obligation,) = result.coverage
    assert obligation.attributions == ("fixture:attr",)
    assert variable.states[0].provenance == "fixture:attr"
    check_delivery_coverage(
        (variable,), result.coverage, withheld=result.withheld_dependencies
    )
    stripped = variable.model_copy(
        update={"states": (variable.states[0].model_copy(update={"provenance": None}),)}
    )
    with pytest.raises(ValueError, match="claimed attributions"):
        check_delivery_coverage(
            (stripped,), result.coverage, withheld=result.withheld_dependencies
        )
