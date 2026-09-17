"""Complete-scope composition checks curation before catalog dependencies."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_parent_key,
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    AliasWindowDecision,
    CodingDecision,
    CurationCase,
    PeerGuard,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import SourceRecord, SourceRevision, value_field
from reg_meta_build.source_scope import resolve_source_scope
from reg_meta_build.source_support import SourceSupportBindings
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


def resolve(records, *, cases=(), naming=None, provider_keys=None, on_diagnostic=None):
    support = SourceSupportBindings((), ())
    for item in records:
        support.observe(item)
    support.seal()
    return resolve_source_scope(
        records,
        cases=cases,
        naming=names(records) if naming is None else naming,
        provider_keys={
            key: str(r.subject.variable.native_id)
            for r in records
            if (key := native_variable_key(r)) is not None
        }
        if provider_keys is None
        else provider_keys,
        value_sessions=(),
        support=support,
        classifications={},
        classification_references={},
        revisions=(REVISION,),
        on_diagnostic=on_diagnostic,
    )


def test_ordinary_scope_forms_variables_with_literal_provider_keys():
    first, second = record(), record(2, variable=6, column="OTHER")
    result = resolve((first, second))
    assert result.evaluations == () and result.diagnostics == ()
    assert len(result.parents.registers) == len(result.parents.variants) == 1
    assert {v.provider_key for v in result.variables.values()} == {"5", "6"}
    assert sum(len(v.states) for v in result.variables.values()) == 2
    assert len(result.corrections.occurrences) == 2


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
