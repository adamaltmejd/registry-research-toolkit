"""Complete-scope composition: partition-scoped coding and classification keys, withheld variants and parallel columns."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_scope_support import guard, names, record, resolve
from reg_meta_build.catalog_dependencies import (
    CatalogDependencies,
    CatalogDependencyError,
    CoverageObligation,
    check_delivery_coverage,
    resolve_panel_dependencies,
    variable_dependency_keys,
)
from reg_meta_build.curation_compile import (
    convert_column_partitions,
)
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationCode,
)
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_curation import (
    AliasWindowDecision,
    CheckedIdentityChange,
    ClassificationDecision,
    CodingDecision,
    CurationCase,
    OccurrenceCorrectionDecision,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
)
from reg_meta_build.source_records import (
    TemporalScope,
    value_field,
)

from reg_meta_build.fqid_slugs import SlugEntry


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
