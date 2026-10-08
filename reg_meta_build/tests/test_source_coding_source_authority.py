"""A guarded extension keeps its source authority through an accepted held column owner."""

from __future__ import annotations

from dataclasses import replace

from _source_coding_choices_support import (
    choice_record as _record,
    compile_entry as _compile_entry,
    list_claim as _claim,
    row_authority as _row_authority,
)
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_curation import (
    SourceEvidence,
    capture_expectations,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    SourceFields,
    value_field,
)


def test_guarded_extend_uses_held_column_owner_without_anchor_coding():
    from reg_meta_build.source_coding import coding_source_sha256

    donor, anchor = _record(2021), _record(2020, "ANCHOR", variable=6)
    claim = _claim("binary", "1", "2021-01-01", "2021-12-31")
    _, _, register, scope, _, column = _compile_entry(
        "extend",
        {"list": "binary", "witness": ["2021-01-01", "2021-12-31"]},
        (claim,),
        record=donor,
    )
    authority = _row_authority(donor, (claim,)).model_copy(
        update={
            "records": list(
                capture_expectations(
                    (donor, anchor),
                    fields=tuple(SourceFields.model_fields),
                    parents=True,
                    coding=True,
                )
            ),
            "locators": [*donor.locators, *anchor.locators],
            "raw_codings": [coding_source_sha256(claim)],
        }
    )
    entry = register.coding.extend[0].model_copy(update={"source_authority": authority})
    register = register.model_copy(
        update={"coding": register.coding.model_copy(update={"extend": [entry]})}
    )
    held = replace(
        source_occurrence(anchor),
        variable_key=source_occurrence(donor).variable_key,
        fields=SourceFields(column_name=value_field("VALUE")),
        source_records=(),
        support_records=(anchor,),
        coding_records=(),
        occurrence_key="accepted-holding",
    )
    occurrences = (source_occurrence(donor), held)
    evidence = SourceEvidence((donor, anchor), effective_occurrences=occurrences)
    assert evidence.effective_scopes is not None
    cases, issues = compile_coding_register(
        register,
        scope,
        originals=(donor, anchor),
        columns={column: (donor, anchor)},
        column_scopes=evidence.effective_scopes,
        coding={column: (claim,)},
    )
    assert len(cases) == 1 and not issues
    result = apply_coding_choices(evidence, cases, coding={column: (claim,)})
    assert not result.diagnostics
    assert result.coding[column].claims == (claim,)
    code_set = result.coding[column].segments[0].code_set
    assert code_set is not None and code_set.members == (("1", "Label"),)
    assert held.variable_key == source_occurrence(donor).variable_key
    assert held.coding_records == ()
    changed = anchor.model_copy(update={"parent_facts": ()})
    assert apply_coding_choices(
        SourceEvidence((donor, changed), effective_occurrences=occurrences),
        cases,
        coding={column: (claim,)},
    ).diagnostics
