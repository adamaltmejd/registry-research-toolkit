"""Guarded coding selections pin complete physical source authority and retain raw claims."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_coding_choices_support import (
    choice_record as _record,
    column_scopes as _column_scopes,
    compile_entry as _compile_entry,
    list_claim as _claim,
    row_authority as _row_authority,
)
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_coding import (
    CodeMembershipClaim,
)
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_curation import (
    SourceEvidence,
    capture_expectations,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "prose",
        "parent",
        "multiplicity",
        "locator",
        "label",
        "token",
        "validity",
        "peer",
        "removed",
        "removed_assertion",
        "column",
        "scope",
    ],
)
@pytest.mark.parametrize("kind", ["choice", "extend"])
def test_guarded_selection_pins_complete_physical_source_authority(drift, kind):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_values import SourceValueAssociation

    record = _record()
    association = SourceValueAssociation(1, "binary", "1", "codes.csv")
    narrow = _claim("narrow", "1")
    keeper = replace(
        (
            _claim("keeper", "1")
            if kind == "choice"
            else _claim("keeper", "1", "2021-01-01", "2021-12-31")
        ),
        members=(
            replace(narrow.members[0], associations=(association,)),
            CodeMembershipClaim(
                ".",
                "Skip",
                TemporalScope(kind="year_independent"),
                associations=(replace(association, row_number=2, value_key="."),),
            ),
        ),
    )
    claims = (narrow, keeper) if kind == "choice" else (keeper,)
    authority = _row_authority(record, claims).model_copy(
        update={"raw_codings": sorted({coding_source_sha256(c) for c in claims})}
    )
    values = (
        {
            "keep": "keeper",
            "keep_members": [["1", "Label"], [".", "Skip"]],
            "over": ["narrow"],
        }
        if kind == "choice"
        else {
            "list": "keeper",
            "list_members": [["1", "Label"], [".", "Skip"]],
            "witness": ["2021-01-01", "2021-12-31"],
        }
    )
    values["source_authority"] = authority
    cases, issues, register, scope, _, column = _compile_entry(kind, values, claims)
    assert len(cases) == 1 and not issues
    assert cases[0].targets == tuple(authority.records)
    records, changed_claims = (record,), claims
    if drift == "prose":
        records = (
            record.model_copy(
                update={
                    "fields": record.fields.model_copy(
                        update={"definition": value_field("Changed")}
                    )
                }
            ),
        )
    elif drift == "parent":
        assert record.parent_facts
        records = (record.model_copy(update={"parent_facts": ()}),)
    elif drift == "peer":
        records += (_record(2021),)
    elif drift == "column":
        records = (
            record.model_copy(
                update={
                    "fields": record.fields.model_copy(
                        update={"column_name": value_field("Other")}
                    )
                }
            ),
        )
    elif drift == "scope":
        records = (
            record.model_copy(
                update={"edition_scope": TemporalScope(kind="unknown", label="Changed")}
            ),
        )
    elif drift == "removed":
        changed_claims = (keeper,) if kind == "choice" else ()
    elif drift is not None:
        member = keeper.members[0]
        if drift == "multiplicity":
            member = replace(member, associations=(association, association))
        elif drift == "removed_assertion":
            member = replace(member, associations=())
        elif drift == "locator":
            member = replace(member, associations=(replace(association, row_number=3),))
        elif drift == "label":
            member = replace(member, label="Changed")
        elif drift == "token":
            member = replace(member, code="2")
        elif drift == "validity":
            member = replace(
                member,
                scope=TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2020-05-01", end="2020-12-31"),),
                ),
            )
        changed = replace(keeper, members=(member, keeper.members[1]))
        changed_claims = (narrow, changed) if kind == "choice" else (changed,)
    fresh, issues = compile_coding_register(
        register,
        scope,
        originals=records,
        columns={column: records},
        column_scopes=_column_scopes({column: records}),
        coding={column: changed_claims},
    )
    replay = apply_coding_choices(
        SourceEvidence(
            records, effective_occurrences=tuple(source_occurrence(r) for r in records)
        ),
        cases,
        coding={column: changed_claims},
    )
    if drift is None:
        assert fresh == cases and not issues
        assert replay.accounting[0].status == "applied"
        assert replay.coding[column].claims == claims
        code_set = replay.coding[column].segments[0].code_set
        assert code_set is not None
        assert set(code_set.members) == {
            ("1", "Label"),
            (".", "Skip"),
        }
    else:
        assert not fresh and issues
        assert replay.accounting[0].status == "stale"


@pytest.mark.parametrize("kind", ["choice", "documented"])
def test_prepared_coding_authority_materializes_all_original_projection_alternatives(
    kind,
):
    from reg_meta_build.source_coding import coding_source_sha256

    record = _record()
    alternative = record.model_copy(
        update={
            "fields": record.fields.model_copy(
                update={"data_length": value_field("10")}
            ),
            "locators": (
                record.locators[0].model_copy(update={"physical_record": "second"}),
            ),
        }
    )
    claims = (
        (_claim("keep", "1"), _claim("other", "2"))
        if kind == "choice"
        else (
            replace(
                _claim("source", "1"),
                members=(
                    CodeMembershipClaim(
                        "1", None, TemporalScope(kind="year_independent")
                    ),
                ),
            ),
        )
    )
    authority = _row_authority(record, claims).model_copy(
        update={
            "records": list(
                capture_expectations(
                    (record, alternative),
                    fields=tuple(SourceFields.model_fields),
                    parents=True,
                    coding=True,
                )
            ),
            "locators": [*record.locators, *alternative.locators],
            "raw_codings": sorted({coding_source_sha256(c) for c in claims})
            if kind == "choice"
            else None,
        }
    )
    values = (
        {"keep": "keep", "over": ["other"]}
        if kind == "choice"
        else {"members": [["1", "Included"]], "version_label": "Supplied row"}
    )
    _, _, register, scope, columns, column = _compile_entry(
        kind,
        {**values, "source_authority": authority},
        claims,
    )
    # Production column membership contains one representative per semantic ref.
    assert columns[column] == (record,)
    cases, issues = compile_coding_register(
        register,
        scope,
        originals=(record, alternative),
        columns=columns,
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert len(cases) == 1 and not issues
    assert cases[0].targets == tuple(authority.records)
    replay = apply_coding_choices(
        SourceEvidence(
            (record, alternative),
            effective_occurrences=(
                source_occurrence(record),
                source_occurrence(alternative),
            ),
        ),
        cases,
        coding={column: claims},
    )
    assert replay.accounting[0].status == "applied"
    changed = alternative.model_copy(
        update={
            "fields": alternative.fields.model_copy(
                update={"data_length": value_field("11")}
            )
        }
    )
    fresh, issues = compile_coding_register(
        register,
        scope,
        originals=(record, changed),
        columns=columns,
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert not fresh and issues
    replay = apply_coding_choices(
        SourceEvidence(
            (record, changed),
            effective_occurrences=(
                source_occurrence(record),
                source_occurrence(changed),
            ),
        ),
        cases,
        coding={column: claims},
    )
    assert replay.accounting[0].status == "stale"


@pytest.mark.parametrize("missing", [None, "authority", "members", "raw_codings"])
def test_unlabelled_extension_requires_complete_literal_source_authority(missing):
    from reg_meta_build.source_coding import coding_source_sha256

    record = _record()
    claim = replace(
        _claim("source", "1", "2021-01-01", "2021-12-31"), version_label=None
    )
    authority = _row_authority(record, (claim,)).model_copy(
        update={"raw_codings": [coding_source_sha256(claim)]}
    )
    values = {
        "list": "",
        "list_members": [["1", "Label"]],
        "witness": ["2021-01-01", "2021-12-31"],
        "source_authority": authority,
    }
    if missing == "authority":
        values.pop("source_authority")
    elif missing == "members":
        values.pop("list_members")
    elif missing == "raw_codings":
        values["source_authority"] = authority.model_copy(update={"raw_codings": None})
    if missing is not None:
        with pytest.raises(ValueError):
            _compile_entry("extend", values, (claim,))
        return
    cases, issues, _, _, _, column = _compile_entry("extend", values, (claim,))
    assert len(cases) == 1 and not issues
    result = apply_coding_choices((record,), cases, coding={column: (claim,)})
    assert not result.diagnostics
    assert result.coding[column].claims == (claim,)
    segment = result.coding[column].segments[0]
    assert segment.code_set is not None
    assert segment.code_set.members == (("1", "Label"),)
    assert segment.version_label == ""


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


@pytest.mark.parametrize(
    "drift", [None, "label", "missing", "code", "association", "book", "historical"]
)
def test_documented_exact_label_equivalence_retains_raw_claims(drift):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import CodeLabelEquivalence
    from reg_meta_build.source_values import SourceValueAssociation

    record = _record()
    association = SourceValueAssociation(1, "descriptor", "value", "values.csv")
    claim = replace(
        _claim("Sector", "14", "2019-01-01", "2019-12-31"),
        members=(
            CodeMembershipClaim(
                "14", "Landsting", TemporalScope(kind="year_independent")
            ),
            CodeMembershipClaim(
                "14",
                "Region",
                TemporalScope(kind="year_independent"),
                associations=(association,),
            ),
            CodeMembershipClaim("15", "Other", TemporalScope(kind="year_independent")),
        ),
    )
    historical = _claim("Old sector", "29", "1968-01-01", "1968-12-31")
    claims = (claim, historical)
    authority = _row_authority(record, claims).model_copy(
        update={
            "raw_codings": [coding_source_sha256(c) for c in claims],
            "label_equivalences": [
                CodeLabelEquivalence(
                    code="14",
                    labels=("Landsting", "Region"),
                    selected_label="Landsting",
                )
            ],
        }
    )
    values = {
        "members": [["14", "Landsting"], ["15", "Other"]],
        "version_label": "Sector",
        "source_authority": authority,
    }
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented", values, claims, record=record
    )
    assert not diagnostics
    changed = claim
    if drift == "label":
        changed = replace(
            claim,
            members=(
                claim.members[0],
                replace(claim.members[1], label="Different"),
                claim.members[2],
            ),
        )
    elif drift == "missing":
        changed = replace(claim, members=(claim.members[0], claim.members[2]))
    elif drift == "code":
        changed = replace(
            claim, members=(*claim.members, replace(claim.members[2], code="16"))
        )
    elif drift == "association":
        changed = replace(
            claim,
            members=(
                claim.members[0],
                replace(
                    claim.members[1], associations=(replace(association, row_number=2),)
                ),
                claim.members[2],
            ),
        )
    if drift == "book":
        changed = replace(claim, version_label="Different book")
    if drift == "historical":
        historical = replace(
            historical,
            members=(replace(historical.members[0], label="Changed old label"),),
        )
    current = (changed, historical)
    result = apply_coding_choices((record,), cases, coding={column: current})
    assert result.coding[column].claims == current
    if drift is None:
        assert not result.diagnostics
        selected = next(
            s for s in result.coding[column].segments if s.valid_from == "2020-01-01"
        )
        assert selected.code_set.members == (("14", "Landsting"), ("15", "Other"))
    else:
        assert result.accounting[0].status == "stale"
        assert _compile_entry("documented", values, current, record=record)[1]
