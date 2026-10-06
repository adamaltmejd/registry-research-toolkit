"""Parallel representations, column corrections, maintained coverage and authored native partitions compile from source coordinates."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from _curation_compile_support import (
    case_record as _case_record,
    checked_correction_fixture as _checked_correction_fixture,
    compile_partition_fixture as _compile_partition_fixture,
    partition_scope as _partition_scope,
    pooled_parallel_fixture as _pooled_parallel_fixture,
    run_checked_correction as _run_checked_correction,
    scb_partition_records as _scb_partition_records,
    scb_partition_tree as _scb_partition_tree,
)
from reg_meta_build.curation_compile import (
    compile_partitions,
    compile_provider_declarations,
    convert_column_partitions,
)
from reg_meta_build.curation_tree import (
    ErrataFieldEntry,
    load_curation_tree,
    load_register_files,
)
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coordinates import (
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    CheckedIdentityChange,
    CuratedOccurrenceAddition,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceFields,
    value_field,
)

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path


def test_parallel_representation_uses_checked_effective_literal_and_raw_guards(
    tmp_path,
):
    from reg_meta_build.curation_compile import compile_parallel_representations
    from reg_meta_build.source_curation import (
        CheckedFieldChange,
        CurationCase,
        FieldExpectation,
        OccurrenceCorrectionDecision,
        capture_expectations,
        evaluate_cases,
    )

    path, records, naming = _pooled_parallel_fixture(tmp_path)
    (register,) = load_register_files(path.parents[2])
    raw = (
        records[0],
        records[1].model_copy(
            update={
                "fields": records[1].fields.model_copy(
                    update={"column_name": value_field("OldSecond")}
                )
            }
        ),
    )
    owner = naming[-1].target.source_key
    targets = capture_expectations(
        raw, fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    correction = CurationCase(
        case_id="reviewed-column",
        targets=targets,
        peer_guards=naming[-1].target.peer_guards,
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=(
                *(
                    CheckedIdentityChange(ref=record_ref(r), variable_key=owner)
                    for r in raw
                ),
                CheckedFieldChange(
                    ref=record_ref(raw[1]),
                    replacement=FieldExpectation(
                        name="column_name", status="value", value="Second"
                    ),
                ),
            ),
            reason="Exact supplied literal correction",
            provenance="fixture",
        ),
    )
    assert compile_parallel_representations(register, raw, naming)[0] == ()
    (case,), issues = compile_parallel_representations(
        register, raw, naming, ownership_cases=(correction,)
    )
    assert issues == ()
    assert evaluate_cases((case,), raw)[0].status == "applicable"
    assert any(
        field.value == "OldSecond"
        for target in case.targets
        for alternative in target.alternatives
        for field in alternative.fields
        if field.name == "column_name"
    )
    changed = (
        raw[0],
        raw[1].model_copy(
            update={
                "fields": raw[1].fields.model_copy(
                    update={"definition": value_field("Changed meaning")}
                )
            }
        ),
    )
    assert evaluate_cases((case,), changed)[0].status == "stale"
    assert (
        compile_parallel_representations(
            register, changed, naming, ownership_cases=(correction,)
        )[0]
        == ()
    )
    empty = register.model_copy(
        update={
            "representation": register.representation.model_copy(
                update={"parallel": []}
            )
        }
    )
    assert compile_parallel_representations(
        empty, changed, naming, ownership_cases=(correction,)
    ) == ((), ())
    before = compile_parallel_representations(register, records, naming)
    assert before == compile_parallel_representations(
        register, records, naming, ownership_cases=()
    )


def test_column_correction_requires_complete_fields_and_preserves_original(tmp_path):
    from reg_meta_build.source_records import SourceFields

    tree, scope, source, _, base = _checked_correction_fixture(tmp_path)
    guards = list(
        capture_expectations((source,), fields=tuple(SourceFields.model_fields))[0]
        .alternatives[0]
        .fields
    )
    entry = ErrataFieldEntry(
        **{
            **base.model_dump(exclude={"field", "value", "expected_fields"}),
            "expected_fields": guards,
        },
        field="column_name",
        value="PHYSICAL",
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (source,))
    assert not issues
    applied = apply_occurrence_cases((source,), cases[scope.source, None])
    assert not applied.diagnostics
    assert applied.occurrences[0].fields.column_name.value == "PHYSICAL"
    assert applied.occurrences[0].source_records == (source,)
    changed = source.model_copy(
        update={
            "fields": source.fields.model_copy(
                update={"data_length": value_field("17")}
            )
        }
    )
    assert apply_occurrence_cases((changed,), cases[scope.source, None]).diagnostics
    _, stale, _ = _run_checked_correction(tree, scope, (changed,))
    assert stale
    for updates in (
        {"expected_fields": guards[:-1]},
        {"edition": None},
        {"value": source.fields.column_name.value},
    ):
        with pytest.raises(ValueError):
            ErrataFieldEntry.model_validate({**entry.model_dump(), **updates})


def test_partition_data_warning_is_explicit_and_scoped_to_annotated_members(
    tmp_path: Path,
):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        '[[identity.partition]]\nvariable = "1.5"\n'
        'columns = { ANSWER = "1.5.answer" }\n'
        'unassigned_columns = ["LEFT"]\ncolumns_ref = "fixture map"\n'
        'data_warning = "Identity follows the exported source question"\n',
    )
    records = _scb_partition_records(("ANSWER", "LEFT"))
    compiled, key, _ = _compile_partition_fixture(root, records)
    (case,) = compiled[0][key]
    assert case.decision.data_warning == "Identity follows the exported source question"
    assert set(case.decision.data_warning_refs) == {
        record_ref(record) for record in records
    }
    assert case.decision.data_warning_fields == ("identity",)


@pytest.mark.parametrize("provider", ["scb", "fk"])
@pytest.mark.parametrize("source_role", ["thin_provider", "scb_records"])
@pytest.mark.parametrize("declared_start", ["2019-01-01", None])
def test_maintained_provider_coverage_uses_input_role_not_provider_name(
    provider,
    source_role,
    declared_start,
):
    parent = _case_record(
        provider=provider,
        register="r",
        parent="register",
        fields=SourceFields(coverage_from=value_field(declared_start))
        if declared_start
        else SourceFields(name=value_field("Unknown coverage register")),
    )
    variable = _case_record(
        provider=provider,
        register="r",
        variable="col",
        fields=SourceFields(
            column_name=value_field("COL"), availability=value_field(True)
        ),
    )
    records = (parent, variable)
    register_key = source_register_key(variable)
    assert register_key is not None
    register = SimpleNamespace(
        register_info=SimpleNamespace(provider=provider, slug="r"),
        source_file=f"curation/registers/{provider}/r.toml",
    )
    scope = CompiledScope(
        source=variable.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register", provider=provider, source_key=register_key
                ),
                naming=SlugEntry(
                    kind="register", provider=provider, source_id="1", slug="r"
                ),
                contributors=(),
            ),
        ),
    )
    prepared = SimpleNamespace(
        value_sources=(),
        manifest=SimpleNamespace(
            inputs=(
                SimpleNamespace(
                    role=source_role, revision=SimpleNamespace(dataset=variable.source)
                ),
            )
        ),
        records=SimpleNamespace(iter_records=lambda *, source: iter(records)),
    )
    if source_role == "thin_provider" and declared_start is None:
        with pytest.raises(ValueError, match="empty or inverted thin coverage window"):
            compile_provider_declarations(
                SimpleNamespace(registers=(register,)), prepared, (scope,), subset=False
            )
        assert parent.parent_facts[0].fields.coverage_from is None
        return
    cases, diagnostics, _ = compile_provider_declarations(
        SimpleNamespace(registers=(register,)),
        prepared,
        (scope,),
        subset=False,
    )
    assert not diagnostics
    if source_role != "thin_provider":
        assert not cases
        return
    result = apply_occurrence_cases(records, cases[scope.source, None])
    addition = next(
        e
        for c in cases[scope.source, None]
        for e in c.decision.effects
        if isinstance(e, CuratedOccurrenceAddition)
    )
    assert addition.edition_period_scope.intervals == (
        ScopeInterval(start=declared_start, end=None),
    )
    assert addition.fields == variable.fields
    assert result.occurrences[1].source_records == (variable,)


@pytest.mark.parametrize(
    "columns, owners, reference",
    [
        ({"OLD": "1.5"}, ("1.5",), "review"),
        ({"OLD": "1.6", "NEW": "1.6"}, ("1.6",), "review"),
        (None, ("1.5",), None),
    ],
)
def test_native_partition_requires_complete_explicit_own_family(
    columns, owners, reference
):
    with pytest.raises(ValueError):
        convert_column_partitions(
            _scb_partition_records(("OLD", "NEW")),
            source_id="1.5",
            split_ids=owners,
            declared_columns=columns,
            declaration_reference=reference,
        )


@pytest.mark.parametrize(
    "columns, unassigned",
    [
        ({"OLD": "1.6"}, []),
        ({"OLD": "1.5", "NEW": "1.5.new"}, []),
        ({"OLD": "1.5"}, ["NEW"]),
    ],
)
def test_authored_native_partition_rejects_foreign_or_partial_owners(
    columns, unassigned
):
    from reg_meta_build.curation_tree import IdentityPartitionEntry

    with pytest.raises(ValueError):
        IdentityPartitionEntry(
            variable="1.5",
            columns=columns,
            unassigned_columns=unassigned,
            columns_ref="reviewed quantity",
        )


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "field",
        "parent",
        "coding",
        "missing_duplicate",
        "partial_digest",
        "added_duplicate",
    ],
)
def test_authored_native_partition_pins_complete_originals_on_fresh_compile(
    tmp_path: Path, drift: str | None
):
    from reg_meta_build.curation_tree import IdentityPartitionEntry
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256
    from reg_meta_build.source_records import CodeSetReference

    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5"\nslug = "quantity"\n'
        '[[identity.partition]]\nvariable = "1.5"\n'
        'columns = { OLD = "1.5", NEW = "1.5" }\n'
        'columns_ref = "complete source-native quantity review"\n',
    )
    original = _scb_partition_records(("OLD", "NEW"))
    duplicate = original[0].model_copy(
        update={
            "record_id": original[0].record_id + "-duplicate",
            "locators": (
                original[0].locators[0].model_copy(update={"physical_record": "99"}),
            ),
        }
    )
    original = (*original, duplicate)
    tree = load_curation_tree(root)
    register = next(r for r in tree.registers if r.identity.partition)
    declaration = register.identity.partition[0]
    guarded = IdentityPartitionEntry.model_validate_json(
        json.dumps(
            {
                **declaration.model_dump(mode="json", exclude_none=True),
                "expected_evidence_sha256": acknowledgement_evidence_sha256(
                    original[:1] if drift == "partial_digest" else original
                ),
            }
        )
    )
    register = register.model_copy(
        update={
            "identity": register.identity.model_copy(update={"partition": [guarded]})
        }
    )
    tree = replace(tree, registers=(register,))
    records = original
    if drift == "missing_duplicate":
        records = original[:-1]
    elif drift == "added_duplicate":
        records = (
            *original,
            duplicate.model_copy(update={"record_id": "new-duplicate"}),
        )
    elif drift in {"field", "parent", "coding"}:
        first = original[0]
        if drift == "field":
            change = {
                "fields": first.fields.model_copy(
                    update={"definition": value_field("different source quantity")}
                )
            }
        elif drift == "parent":
            parent = first.parent_facts[0]
            change = {
                "parent_facts": (
                    parent.model_copy(
                        update={
                            "fields": parent.fields.model_copy(
                                update={"name": value_field("different source parent")}
                            )
                        }
                    ),
                    *first.parent_facts[1:],
                )
            }
        else:
            change = {
                "code_set_references": (
                    CodeSetReference(
                        reference_id="different-source-list",
                        content_sha256="0" * 64,
                        physical_locator="different source list cell",
                    ),
                )
            }
        records = (first.model_copy(update=change), *original[1:])
    native = native_variable_key(records[0])
    reader = SimpleNamespace(
        iter_partition_families=lambda *a, **k: iter(((native, records),))
    )
    scope = _partition_scope(records)
    compiled = compile_partitions(
        tree, cast("Any", SimpleNamespace(records=reader)), (scope,)
    )
    key = (scope.source, None)
    cases, naming, keys, _, _, _, issues = compiled
    if drift is not None:
        assert {d.code for d in issues} == {"stale_curation_entry"}
        assert not cases.get(key) and not naming.get(key)
        assert keys[key] == ((native, None),)
    else:
        assert not issues
        assert (
            cases[key][0].expected_evidence_sha256 == guarded.expected_evidence_sha256
        )
        result = apply_occurrence_cases(records, cases[key])
        assert not result.diagnostics and len(result.occurrences) == len(original)
        assert all(
            o.identity_checked and o.variable_key == native for o in result.occurrences
        )
        stale = apply_occurrence_cases(original[:-1], cases[key])
        assert stale.diagnostics and not any(
            o.identity_checked for o in stale.occurrences
        )


@pytest.mark.parametrize("digest", ["not-a-sha", "A" * 64, "a" * 63])
def test_authored_partition_requires_a_complete_digest(digest: str):
    from reg_meta_build.curation_tree import IdentityPartitionEntry

    with pytest.raises(ValueError):
        IdentityPartitionEntry(
            variable="1.5",
            columns={"OLD": "1.5", "NEW": "1.5"},
            columns_ref="reviewed complete family",
            expected_evidence_sha256=digest,
        )


def test_delivery_metadata_rejects_redundant_unit_permission():
    from reg_meta_build.curation_tree import DeliveryMetadataEntry
    from reg_meta_build.source_curation import DeliveryMetadataDecision

    with pytest.raises(ValueError):
        DeliveryMetadataEntry(
            fields=["measurement_unit"],
            variable="1.5",
            records=[],
            columns=[],
            evidence="source",
            noted="2026-10-01",
        )
    with pytest.raises(ValueError, match="name or description"):
        DeliveryMetadataDecision.require_fields(("measurement_unit",))
