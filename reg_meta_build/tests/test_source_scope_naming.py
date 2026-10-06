"""Complete-scope composition: ambiguous names, checked naming and native provider keys."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_scope_support import guard, names, record, resolve
from reg_meta.source_evidence import SourceField, canonical_sha256
from reg_meta_build.catalog_dependencies import (
    CatalogDependencies,
    CatalogDependencyError,
    variable_dependency_keys,
)
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    CheckedIdentityChange,
    CodingDecision,
    CurationCase,
    OccurrenceCorrectionDecision,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_naming import (
    AcceptedNamingEntry,
    NamingAmbiguity,
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    value_field,
)

from reg_meta_build.fqid_slugs import SlugEntry


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
