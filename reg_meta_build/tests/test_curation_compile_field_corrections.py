"""Representation and source-attribution corrections apply to every guarded original.

Both are kept at compile level: the build-case runner projects neither a state's
representation nor its source attribution, so no boundary case can observe the
corrected value. Their load refusals are `cases/curation_toml/errata-field-*`.
"""

from __future__ import annotations

from dataclasses import replace

from _curation_compile_support import (
    checked_correction_fixture as _checked_correction_fixture,
    run_checked_correction as _run_checked_correction,
)
from reg_meta_build.curation_tree import ErrataFieldEntry
from reg_meta_build.source_curation import capture_expectations
from reg_meta_build.source_effects import apply_occurrence_cases
from reg_meta_build.source_records import (
    SourceFields,
    TemporalScope,
    value_field,
)


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
