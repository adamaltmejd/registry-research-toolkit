"""Field, attribution and name corrections and guarded SOS splits compile against complete originals."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from _curation_compile_support import (
    checked_correction_fixture as _checked_correction_fixture,
    make_revision as _revision,
    make_tree as _tree,
    partition_scope as _partition_scope,
    pooled_parallel_fixture as _pooled_parallel_fixture,
    run_checked_correction as _run_checked_correction,
    sos_partition_records as _sos_partition_records,
)
from reg_meta_build.curation_compile import (
    compile_partitions,
)
from reg_meta_build.curation_tree import (
    ErrataFieldEntry,
    load_curation_tree,
    load_register_files,
)
from reg_meta_build.source_coordinates import (
    native_variable_key,
)
from reg_meta_build.source_curation import (
    CheckedIdentityChange,
    capture_expectations,
    evaluate_cases,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_records import (
    SourceFields,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path


def test_field_correction_accepts_guarded_source_alternatives_without_losing_originals(
    tmp_path,
):
    tree, scope, source, _, base = _checked_correction_fixture(tmp_path)
    source = source.model_copy(
        update={
            "fields": source.fields.model_copy(
                update={"representation": value_field("Source reference")}
            )
        }
    )
    peer = source.model_copy(
        update={
            "record_id": source.record_id + ":peer",
            "fields": source.fields.model_copy(
                update={
                    "representation": value_field(
                        "Source reference https://example.org"
                    )
                }
            ),
        }
    )
    fields = (
        "name",
        "definition",
        "description",
        "operational_definition",
        "classification_declared",
        "representation",
        "data_type",
        "coverage_from",
        "coverage_to",
    )
    entry = ErrataFieldEntry(
        **base.model_dump(
            exclude={"field", "value", "expected_fields", "expected_records"}
        ),
        field="representation",
        value=peer.fields.representation.value,
        expected_fields=list(
            capture_expectations((source,), fields=fields)[0].alternatives[0].fields
        ),
        expected_records=list(
            capture_expectations(
                (source, peer),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
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
    cases, issues, _ = _run_checked_correction(tree, scope, (source, peer))
    assert not issues
    applied = apply_occurrence_cases((source, peer), cases[scope.source, None])
    assert not applied.diagnostics
    assert all(
        o.fields.representation.value == entry.value for o in applied.occurrences
    )
    assert {r for o in applied.occurrences for r in o.source_records} == {source, peer}
    _, issues, _ = _run_checked_correction(tree, scope, (source,))
    assert issues
    for changed_field in ("representation", "description", "identifier"):
        changed = peer.model_copy(
            update={
                "fields": peer.fields.model_copy(
                    update={
                        changed_field: value_field(False)
                        if changed_field == "identifier"
                        else value_field("CHANGED")
                    }
                )
            }
        )
        _, issues, _ = _run_checked_correction(tree, scope, (source, changed))
        assert issues
        assert apply_occurrence_cases(
            (source, changed), cases[scope.source, None]
        ).diagnostics
    changed = peer.model_copy(
        update={"edition_scope": TemporalScope(kind="year_independent")}
    )
    _, issues, _ = _run_checked_correction(tree, scope, (source, changed))
    assert issues
    for updates in (
        {"expected_records": entry.expected_records[:-1]},
        {"value": "Unsupplied replacement"},
        {"expected_fields": entry.expected_fields[:-1]},
    ):
        with pytest.raises(ValueError):
            ErrataFieldEntry.model_validate({**entry.model_dump(), **updates})


@pytest.mark.parametrize("native_base", [False, True])
@pytest.mark.parametrize("shared_ref", [False, True])
def test_parallel_representation_selects_checked_owner_without_literal_changes(
    tmp_path,
    native_base,
    shared_ref,
):
    from reg_meta_build.curation_compile import compile_parallel_representations
    from reg_meta_build.source_curation import (
        CurationCase,
        OccurrenceCorrectionDecision,
        capture_expectations,
        evaluate_cases,
    )

    path, selected, naming = _pooled_parallel_fixture(tmp_path)
    header = REGISTERINFORMATION_HEADER.split("|")
    values = var_row(
        colname="Sibling",
        cvid=102,
        var_id=1,
        varname="Income",
        year="2022",
        versionname="2022",
        regver_id=112,
    ).split("|")
    sibling = clean_scb_row(
        header,
        3,
        {
            name: (True, value, value)
            for name, value in zip(header, values, strict=True)
        },
        _revision("fixture"),
    ).record
    if shared_ref:
        sibling = sibling.model_copy(update={"locators": selected[0].locators})
    records = (*selected, sibling)
    owner = (
        native_variable_key(selected[0])
        if native_base
        else naming[-1].target.source_key
    )
    assert owner is not None
    targets = capture_expectations(
        records, fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    guards = tuple(
        guard.model_copy(
            update={
                "expected_members": tuple(dict.fromkeys(record_ref(r) for r in records))
            }
        )
        for guard in naming[-1].target.peer_guards
    )
    naming = (
        *naming[:-1],
        naming[-1].model_copy(
            update={
                "target": naming[-1].target.model_copy(
                    update={
                        "source_key": owner,
                        "expectations": targets,
                        "peer_guards": guards,
                    }
                ),
            }
        ),
    )
    correction = CurationCase(
        case_id="reviewed-complete-partition",
        targets=targets,
        peer_guards=guards,
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(
                CheckedIdentityChange(
                    ref=record_ref(r),
                    variable_key=owner if r in selected else (*owner, "sibling"),
                    when=capture_expectations((r,), fields=("column_name",))[0]
                    .alternatives[0]
                    .fields,
                )
                for r in records
            ),
            reason="Complete literal role partition",
            provenance="fixture",
        ),
    )
    (register,) = load_register_files(path.parents[2])
    if not shared_ref:
        assert compile_parallel_representations(register, records, naming)[0] == ()
    (case,), issues = compile_parallel_representations(
        register, records, naming, ownership_cases=(correction,)
    )
    assert issues == ()
    if shared_ref:
        assert (
            len(
                next(
                    target
                    for target in case.targets
                    if target.ref == record_ref(sibling)
                ).alternatives
            )
            == 2
        )
    assert {target.ref for target in case.targets} == {record_ref(r) for r in selected}
    assert record_ref(sibling) in {target.ref for target in case.support}
    assert evaluate_cases((case,), records)[0].status == "applicable"
    assert evaluate_cases((case,), selected)[0].status == "stale"
    assert (
        compile_parallel_representations(
            register, selected, naming, ownership_cases=(correction,)
        )[0]
        == ()
    )
    changed = (
        *selected,
        sibling.model_copy(
            update={
                "fields": sibling.fields.model_copy(
                    update={"definition": value_field("Changed role")}
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


def test_source_attribution_correction_requires_complete_originals_and_preserves_raw(
    tmp_path,
):
    tree, scope, source, _, base = _checked_correction_fixture(tmp_path)
    source = source.model_copy(
        update={
            "fields": source.fields.model_copy(
                update={"source_attribution": value_field("Register : Variant (FE)")}
            )
        }
    )
    peer = source.model_copy(
        update={
            "record_id": source.record_id + ":peer",
            "subject": source.subject.model_copy(
                update={
                    "native": source.subject.native.model_copy(
                        update={"member_id": 999}
                    )
                }
            ),
        }
    )
    expectations = list(
        capture_expectations(
            (source, peer),
            fields=tuple(SourceFields.model_fields),
            parents=True,
            coding=True,
        )
    )
    entry = ErrataFieldEntry(
        **base.model_dump(
            exclude={"field", "value", "expected_fields", "expected_records"}
        ),
        field="source_attribution",
        value="Register : Variant (företagsenhet)",
        expected_fields=list(expectations[0].alternatives[0].fields),
        expected_records=expectations,
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
    cases, issues, _ = _run_checked_correction(tree, scope, (source, peer))
    assert not issues
    applied = apply_occurrence_cases((source, peer), cases[scope.source, None])
    assert not applied.diagnostics
    assert all(
        o.fields.source_attribution.value == entry.value for o in applied.occurrences
    )
    assert {r for o in applied.occurrences for r in o.source_records} == {source, peer}
    for records in (
        (source,),
        (
            source,
            peer.model_copy(
                update={
                    "fields": peer.fields.model_copy(
                        update={"source_attribution": value_field("Different source")}
                    )
                }
            ),
        ),
    ):
        _, issues, _ = _run_checked_correction(tree, scope, records)
        assert issues
        assert apply_occurrence_cases(records, cases[scope.source, None]).diagnostics
    for updates in (
        {"expected_records": None},
        {"edition": None},
        {"expected_fields": entry.expected_fields[:-1]},
    ):
        with pytest.raises(ValueError):
            ErrataFieldEntry.model_validate({**entry.model_dump(), **updates})


@pytest.mark.parametrize("drift", [None, "description", "coding", "missing_duplicate"])
def test_guarded_sos_split_checks_complete_physical_family(tmp_path: Path, drift):
    from reg_meta_build.curation_tree import IdentitySplitEntry
    from reg_meta_build.source_curation import (
        acknowledgement_evidence_sha256,
        capture_expectations,
    )
    from reg_meta_build.source_records import CodeSetReference

    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/sos/par.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "sos"\nslug = "par"\n'
        'native_id = "5891427617861710725"\nname = "Patientregistret"\n'
        '[[identity.split]]\nvariable = "ATC"\nby = "deldatamangd"\n'
        'parts = [{ deldatamangd = "PAR_OV", owner = "5891427617861710725.ATC.outpatient" },'
        '{ deldatamangd = "PAR_SV", owner = "5891427617861710725.ATC.inpatient" }]\n'
        '[[variable]]\nnative_id = "5891427617861710725.ATC.outpatient"\nslug = "outpatient"\n'
        '[[variable]]\nnative_id = "5891427617861710725.ATC.inpatient"\nslug = "inpatient"\n'
    )
    original = _sos_partition_records(subsets=("PAR_OV", "PAR_SV"))
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
    register = next(r for r in tree.registers if r.register_info.provider == "sos")
    declaration = register.identity.split[0]
    guarded = IdentitySplitEntry.model_validate_json(
        json.dumps(
            {
                **declaration.model_dump(mode="json", exclude_none=True),
                "expected_records": [
                    e.model_dump(mode="json")
                    for e in capture_expectations(
                        original,
                        fields=tuple(SourceFields.model_fields),
                        parents=True,
                        coding=True,
                    )
                ],
                "expected_evidence_sha256": acknowledgement_evidence_sha256(original),
                "data_warning": "Separate source-defined clinical contexts; no equivalence inferred.",
            }
        )
    )
    register = register.model_copy(
        update={"identity": register.identity.model_copy(update={"split": [guarded]})}
    )
    tree = replace(tree, registers=(register,))
    records = original
    if drift == "missing_duplicate":
        records = original[:-1]
    elif drift is not None:
        changed = original[0].model_copy(
            update={
                "fields": original[0].fields.model_copy(
                    update={"description": value_field("different role")}
                )
            }
            if drift == "description"
            else {
                "code_set_references": (
                    CodeSetReference(
                        reference_id="changed",
                        content_sha256="0" * 64,
                        physical_locator="changed",
                    ),
                )
            }
        )
        records = (changed, *original[1:])
    native = native_variable_key(records[0])
    reader = SimpleNamespace(
        iter_partition_families=lambda *a, **kw: iter(((native, records),))
    )
    scope = _partition_scope(records)
    cases, names, _, _, _, _, diagnostics = compile_partitions(
        tree, cast("Any", SimpleNamespace(records=reader)), (scope,)
    )
    key = (scope.source, None)
    if drift is not None:
        assert [d.code for d in diagnostics] == ["stale_curation_entry"]
        assert not cases.get(key) and not names.get(key)
    else:
        assert not diagnostics
        assert cases[key][0].decision.data_warning == guarded.data_warning
        corrected = apply_occurrence_cases(records, cases[key])
        assert not corrected.diagnostics
        assert len(corrected.occurrences) == 3
        assert evaluate_cases(cases[key], original[:-1])[0].status == "stale"
        assert {o.variable_key[-1] for o in corrected.occurrences} == {
            "PAR_OV",
            "PAR_SV",
        }


def test_name_correction_uses_only_complete_same_family_witnesses(tmp_path):
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256

    tree, scope, target, witness, base = _checked_correction_fixture(tmp_path)
    witness = witness.model_copy(
        update={
            "fields": witness.fields.model_copy(
                update={"name": value_field("Canonical source label")}
            )
        }
    )
    duplicate = witness.model_copy(
        update={
            "record_id": witness.record_id + "-duplicate",
            "locators": (
                witness.locators[0].model_copy(update={"physical_record": "99"}),
            ),
        }
    )
    originals = (target, witness, duplicate)
    entry = ErrataFieldEntry(
        **base.model_dump(
            exclude={
                "field",
                "value",
                "expected_records",
                "authority_records",
                "expected_evidence_sha256",
            }
        ),
        field="name",
        value="Canonical source label",
        expected_records=list(
            capture_expectations(
                (target,),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        authority_records=list(
            capture_expectations(
                (witness, duplicate),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        expected_evidence_sha256=acknowledgement_evidence_sha256(originals),
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
    cases, issues, _ = _run_checked_correction(tree, scope, originals)
    assert not issues
    applied = apply_occurrence_cases(originals, cases[scope.source, None])
    assert not applied.diagnostics
    assert all(o.fields.name.value == entry.value for o in applied.occurrences)
    assert {r for o in applied.occurrences for r in o.source_records} == set(originals)
    for changed in (
        (target,),
        (target, witness),
        (
            target,
            witness.model_copy(
                update={
                    "fields": witness.fields.model_copy(
                        update={"description": value_field("Changed witness role")}
                    )
                }
            ),
            duplicate,
        ),
    ):
        assert _run_checked_correction(tree, scope, changed)[1]
        assert evaluate_cases(cases[scope.source, None], changed)[0].status == "stale"
    foreign = witness.model_copy(
        update={
            "subject": witness.subject.model_copy(
                update={
                    "variable": witness.subject.variable.model_copy(
                        update={"native_id": 999}
                    )
                }
            )
        }
    )
    with pytest.raises(ValueError, match="same native family"):
        ErrataFieldEntry.model_validate_json(
            json.dumps(
                {
                    **entry.model_dump(mode="json", exclude_none=True),
                    "authority_records": [
                        e.model_dump(mode="json")
                        for e in capture_expectations(
                            (foreign,),
                            fields=tuple(SourceFields.model_fields),
                            parents=True,
                            coding=True,
                        )
                    ],
                }
            )
        )
