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
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    CheckedIdentityChange,
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


def test_partial_name_keeps_supported_facts_and_only_names_observed_omissions():
    """Kept as a unit test by maintainer decision (#1267): no build reaches it,
    because a build never makes an ambiguity's names available. Input: an
    ambiguous family with only variant 2's row converted to the named partition.
    Expected: the variable keeps its one supported state; exactly the observed
    variant 3 states, its Code representation and succession representation are
    withheld with variant 3's ref; an unobserved variant 4 is missing, not
    withheld. Fails if the partial-name branch withholds the whole variable, or
    names omissions for variants it never observed.
    """
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
    """Kept as a unit test by maintainer decision (#1267): no build reaches it,
    because the compiler builds each ambiguity from the same build's records, so
    its bridge always matches its family. Input: the family's column renamed, a
    new peer spelling, or the bridge pointing at another native variable.
    Refusal: "ambiguous naming bridge is stale or belongs to another family".
    Fails if resolve_source_scope applies a bridge without comparing it to the
    family's current records.
    """
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
    """Kept as a unit test by maintainer decision (#1267): no build reaches it,
    because the compile always gives an ambiguous family an explicit unresolved
    provider key. Input: an ambiguity with no provider key, or with a known one.
    Refusal: "missing explicit provider key" and "lacks an unresolved native
    identity". Fails if an ambiguity can stand in for a missing conversion or
    override a resolved identity.
    """
    item = record()
    pending = ambiguity((item,))
    with pytest.raises(ValueError, match="missing explicit provider key"):
        resolve((item,), naming_ambiguities=(pending,), provider_keys={})
    with pytest.raises(ValueError, match="lacks an unresolved native identity"):
        resolve((item,), naming_ambiguities=(pending,))


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
