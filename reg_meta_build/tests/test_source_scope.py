"""Complete-scope composition checks curation before catalog dependencies."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from _prepared_fixtures import accept_prepared
from reg_meta_build.catalog_dependencies import (
    CatalogDependencies,
    CatalogDependencyError,
    resolve_panel_dependencies,
    variable_dependency_keys,
)
from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.prepared_values import (
    open_prepared_source_values,
    prepare_source_values,
)
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_parent_key,
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    AliasWindowDecision,
    CheckedIdentityChange,
    CodingDecision,
    CurationCase,
    OccurrenceCorrectionDecision,
    PeerGuard,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_naming import (
    AcceptedNamingEntry,
    NamingAmbiguity,
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    SourceRecord,
    SourceRevision,
    TemporalScope,
    canonical_sha256,
    value_field,
)
from reg_meta_build.source_scope import resolve_source_scope
from reg_meta_build.source_support import SourceSupportBindings
from reg_meta_build.source_value_bindings import open_value_bindings
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
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


def record(member=1, variable=5, variant=2, column="VALUE"):
    header = REGISTERINFORMATION_HEADER.split("|")
    values = _var_row(
        cvid=member,
        var_id=variable,
        colname=column,
        register=("TEST", 1, variant),
        regver_id=2020,
        year="2020",
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
        result.append(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind=kind,
                    provider="scb",
                    source_key=key,
                    register_key=source_register_key(first)
                    if kind != "register"
                    else None,
                    identity_revision=REVISION,
                ),
                naming=SlugEntry(
                    kind=kind,
                    provider="scb",
                    source_id="1" if kind == "register" else f"1.{member_key}",
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
    on_diagnostic=None,
    value_sessions=(),
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
        value_sessions=value_sessions,
        support=support,
        classifications={},
        classification_references={},
        revisions=(REVISION,),
        on_diagnostic=on_diagnostic,
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
                revision=REVISION,
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


def test_stale_parent_name_withholds_dependents_but_is_not_a_contract_error():
    item = record()
    selected = names((item,))
    stale_revision = REVISION.model_copy(update={"upstream_revision": "other"})
    selected = tuple(
        n.model_copy(
            update={
                "target": n.target.model_copy(
                    update={"identity_revision": stale_revision}
                )
            }
        )
        if n.target.kind == "register"
        else n
        for n in selected
    )
    result = resolve((item,), naming=selected)
    assert result.variables == {native_variable_key(item): None}
    assert {d.code for d in result.diagnostics} == {
        "naming_identity_revision_changed",
        "withheld_parent_naming",
        "withheld_register_dependency",
    }
    assert set(result.withheld_dependencies) == {
        ("register", "scb/example"),
        ("variant", "scb/example", "people-2"),
        ("variable", "scb/example/value-5"),
    }
    assert all(
        cause.refs == (record_ref(item),)
        for causes in result.withheld_dependencies.values()
        for cause in causes
    )


def test_missing_conversion_is_fatal():
    item = record()
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve((item,), provider_keys={})
    with pytest.raises(ValueError, match="missing checked parent naming"):
        resolve((item,), naming=())


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
