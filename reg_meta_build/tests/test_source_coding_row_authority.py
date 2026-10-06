"""Documented source rows guard coding through fresh compile and replay, and enumerated meanings need a complete marker certificate."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_coding_choices_support import (
    choice_record as _record,
    column_scopes as _column_scopes,
    compile_entry as _compile_entry,
    documented_values as _documented_values,
    list_claim as _claim,
    row_authority as _row_authority,
)
from pydantic import ValidationError
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_coding import (
    CodeMembershipClaim,
)
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_curation import (
    SourceEvidence,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    TemporalScope,
    value_field,
)


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "prose",
        "scope",
        "revision",
        "coding",
        "label",
        "peer",
        "missing",
        "locator",
        "incomplete_authority",
        "mixed_authority",
        "pdf_open",
        "pdf_missing_page",
        "added_documented_code",
    ],
)
def test_documented_source_rows_guard_fresh_compile_and_replay(drift):
    record = _record()
    claims = (
        replace(
            _claim("source", "1"),
            members=(
                CodeMembershipClaim("1", None, TemporalScope(kind="year_independent")),
            ),
        ),
    )
    authority = _row_authority(record, claims)
    values = {
        "members": [["1", "Included"]],
        "version_label": "Supplied row",
        "source_authority": authority,
    }
    if drift in {
        "incomplete_authority",
        "mixed_authority",
        "pdf_open",
        "pdf_missing_page",
        "added_documented_code",
    }:
        if drift == "incomplete_authority":
            raw = authority.model_dump(mode="json")
            raw["records"][0]["alternatives"][0]["fields"].pop()
            values["source_authority"] = raw
        elif drift == "mixed_authority":
            values.update(_documented_values())
        else:
            values = _documented_values()
            if drift == "pdf_open":
                values["periods"] = [["2020-01-01", "9999-12-31"]]
            else:
                values.pop("document_pages")
        with pytest.raises(ValidationError):
            _compile_entry("documented", values, claims)
        return
    cases, issues, register, scope, _, column = _compile_entry(
        "documented", values, claims
    )
    assert len(cases) == 1 and not issues
    changed = record
    changed_claims = claims
    records = (record,)
    if drift == "prose":
        changed = record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"description": value_field("Changed")}
                )
            }
        )
    elif drift == "scope":
        changed = record.model_copy(
            update={"edition_scope": TemporalScope(kind="unknown", label="Changed")}
        )
    elif drift == "revision":
        changed = record.model_copy(update={"source_revision_id": "changed"})
    elif drift == "coding":
        changed_claims = (
            replace(
                claims[0],
                members=(
                    *claims[0].members,
                    CodeMembershipClaim(
                        "2", None, TemporalScope(kind="year_independent")
                    ),
                ),
            ),
        )
    elif drift == "label":
        changed_claims = (
            replace(
                claims[0],
                members=(replace(claims[0].members[0], label="Contradictory"),),
            ),
        )
    elif drift == "locator":
        changed = record.model_copy(
            update={
                "locators": (
                    record.locators[0].model_copy(
                        update={"physical_record": "changed"}
                    ),
                )
            }
        )
    elif drift == "missing":
        records = ()
    elif drift == "peer":
        records = (record, _record(2021))
    if drift not in {"peer", "missing"}:
        records = (changed,)
    if drift == "added_documented_code":
        entry = register.coding.documented[0].model_copy(
            update={"members": (("1", "Included"), ("0", "Absent invented token"))}
        )
        register = register.model_copy(
            update={
                "coding": register.coding.model_copy(update={"documented": [entry]})
            }
        )
    fresh, diagnostics = compile_coding_register(
        register,
        scope,
        originals=records,
        columns={column: records},
        column_scopes=_column_scopes({column: records}),
        coding={column: changed_claims},
    )
    if drift is None:
        assert fresh == cases and not diagnostics
    else:
        assert not fresh and diagnostics[0].code == "stale_curation_entry"
    replay = apply_coding_choices(records, cases, coding={column: changed_claims})
    if drift in {"prose", "scope", "coding", "label", "peer", "missing"}:
        assert replay.accounting[0].status == "stale"
    elif drift is None:
        assert replay.accounting[0].status == "applied"
        assert replay.coding[column].claims == claims
        code_set = replay.coding[column].segments[0].code_set
        assert code_set is not None
        assert code_set.members == (("1", "Included"),)
        assert "records.csv" in replay.coding[column].segments[0].provenance[0]


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "enlarge",
        "shorten",
        "close",
        "wrong_claim",
        "prose",
        "removed_delivery",
        "mixed_periods",
        "unknown",
        "literal_9999",
    ],
)
def test_source_row_coding_follows_exact_supplied_open_scope(drift):
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="1900", end=None),)
    )
    record = _record().model_copy(
        update={"edition_scope": scope, "edition_period_scope": scope}
    )
    claims = (
        replace(
            _claim("source", "J"),
            scope=scope,
            members=(
                CodeMembershipClaim("J", None, TemporalScope(kind="year_independent")),
                CodeMembershipClaim("N", None, TemporalScope(kind="year_independent")),
            ),
        ),
    )
    authority = _row_authority(record, claims, scope)
    values = {
        "members": [["J", "ja"], ["N", "nej"]],
        "version_label": "Supplied J/N",
        "periods": [],
        "source_authority": authority,
    }
    if drift in {"mixed_periods", "unknown", "literal_9999"}:
        if drift == "mixed_periods":
            values["periods"] = [["1900-01-01", "2000-12-31"]]
        else:
            raw = authority.model_dump(mode="json")
            raw["source_scope"] = (
                TemporalScope(kind="unknown", label="unknown").model_dump(mode="json")
                if drift == "unknown"
                else TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="1900", end="9999-12-31"),),
                ).model_dump(mode="json")
            )
            values["source_authority"] = raw
        with pytest.raises(ValidationError):
            _compile_entry("documented", values, claims, record=record)
        return
    cases, issues, register, compiled_scope, columns, column = _compile_entry(
        "documented", values, claims, record=record
    )
    assert len(cases) == 1 and not issues
    case = cases[0]
    assert case.decision.selection.source_scope == scope
    assert case.decision.selection.source_scope.intervals[0].end is None
    assert case.decision.valid_to == "9999-12-31"
    actual_record = record
    actual_claims = claims
    actual_scopes = _column_scopes(columns)
    if drift in {"enlarge", "shorten", "close"}:
        changed_scope = TemporalScope(
            kind="intervals",
            intervals=(
                ScopeInterval(
                    start="1899"
                    if drift == "enlarge"
                    else "1901"
                    if drift == "shorten"
                    else "1900",
                    end="2000" if drift == "close" else None,
                ),
            ),
        )
        actual_record = record.model_copy(
            update={
                "edition_scope": changed_scope,
                "edition_period_scope": changed_scope,
            }
        )
        actual_scopes = {column: frozenset((changed_scope,))}
    elif drift == "wrong_claim":
        actual_claims = (
            replace(
                claims[0],
                scope=TemporalScope(
                    kind="intervals", intervals=(ScopeInterval(start="1901", end=None),)
                ),
            ),
        )
    elif drift == "prose":
        actual_record = record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"description": value_field("changed")}
                )
            }
        )
    elif drift == "removed_delivery":
        actual_scopes = {column: frozenset()}
    fresh, diagnostics = compile_coding_register(
        register,
        compiled_scope,
        originals=(actual_record,),
        columns={column: (actual_record,)},
        column_scopes=actual_scopes,
        coding={column: actual_claims},
    )
    if drift is None:
        assert fresh == cases and not diagnostics
    else:
        assert not fresh and diagnostics
    evidence = SourceEvidence(
        (actual_record,),
        effective_occurrences=()
        if drift == "removed_delivery"
        else (source_occurrence(actual_record),),
    )
    result = apply_coding_choices(evidence, cases, coding={column: actual_claims})
    if drift is None:
        resolved = result.coding[column]
        assert resolved.claims == claims
        (segment,) = resolved.segments
        assert segment.code_set is not None
        assert segment.code_set.members == (("J", "ja"), ("N", "nej"))
        assert segment.valid_from == "1900-01-01" and segment.valid_to == "9999-12-31"
        assert resolved.claims[0].scope.intervals[0].end is None
    else:
        assert result.accounting[0].status == "stale"


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "prose",
        "partial",
        "binding",
        "missing_binding",
        "outside_labels",
        "finite",
        "peer",
        "missing",
        "scope",
    ],
)
def test_enumerated_source_meanings_require_complete_marker_certificate(drift):
    from reg_meta_build.source_value_bindings import (
        ValueListBinding,
        marker_binding_fingerprints,
    )
    from reg_meta_build.source_values import SourceValueAssociation

    record = _record().model_copy(
        update={
            "fields": _record().fields.model_copy(
                update={
                    "definition": value_field(
                        "Which amount?\n1. Less than 500\n2. At least 500\n8. Unknown"
                    )
                }
            )
        }
    )
    scope = record.edition_scope
    marker = SourceValueAssociation(1, "Tal", "marker", "codes.csv")
    binding = ValueListBinding(
        None, record.record_id, record.locators, "revision", "Tal", 1, (), (marker,)
    )
    bound = ((scope, binding),)
    fingerprints = marker_binding_fingerprints(bound, "2020-01-01", "2020-12-31")
    assert fingerprints
    authority = _row_authority(record, (_claim("irrelevant", "1"),)).model_dump(
        mode="json"
    )
    base_claims = (
        (_claim("later", "1", "2021-01-01", "2021-12-31"),)
        if drift == "outside_labels"
        else ()
    )
    from reg_meta_build.source_coding import copied_coding_fingerprints

    authority.update(
        codings=list(copied_coding_fingerprints(base_claims)),
        enumeration={
            "field": "definition",
            "syntax": "ascii-decimal-dot-space",
            "lines": ["1. Less than 500", "2. At least 500", "8. Unknown"],
        },
        marker_bindings=list(fingerprints),
    )
    values = {
        "members": [["1", "Less than 500"], ["2", "At least 500"], ["8", "Unknown"]],
        "version_label": "Exact enumerated source",
        "source_authority": authority,
    }
    if drift == "partial":
        values["members"].pop()
        authority["enumeration"]["lines"].pop()
    # Missing binding evidence must never act as a compatibility bypass.
    cases, issues, register, compiled_scope, _, column = _compile_entry(
        "documented", values, base_claims, record=record
    )
    assert not cases and issues
    cases, issues = compile_coding_register(
        register,
        compiled_scope,
        originals=(record,),
        columns={column: (record,)},
        column_scopes=_column_scopes({column: (record,)}),
        coding={column: base_claims},
        value_bindings={column: bound},
    )
    if drift == "partial":
        assert not cases and issues
        return
    assert len(cases) == 1 and not issues
    records, changed_bound, claims = (record,), bound, base_claims
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
    elif drift == "binding":
        changed_bound = ((scope, replace(binding, descriptor_key="Changed")),)
    elif drift == "missing_binding":
        changed_bound = ()
    elif drift == "finite":
        changed_bound = ((scope, replace(binding, claim_id="new finite")),)
        claims = (_claim("new", "1"),)
    elif drift == "peer":
        records += (_record(2021),)
    elif drift == "missing":
        records = ()
    elif drift == "scope":
        changed_bound = ((TemporalScope(kind="unknown", label="changed"), binding),)
    fresh, issues = compile_coding_register(
        register,
        compiled_scope,
        originals=records,
        columns={column: records},
        column_scopes=_column_scopes({column: records}),
        coding={column: claims},
        value_bindings={column: changed_bound},
    )
    evidence = SourceEvidence(
        records,
        effective_occurrences=tuple(source_occurrence(r) for r in records),
        value_bindings={column: changed_bound},
    )
    replay = apply_coding_choices(evidence, cases, coding={column: claims})
    if drift in {None, "outside_labels"}:
        assert fresh == cases and not issues
        assert replay.accounting[0].status == "applied"
        assert replay.coding[column].claims == base_claims
        code_set = replay.coding[column].segments[0].code_set
        assert code_set is not None
        assert code_set.members == tuple(map(tuple, values["members"]))
    else:
        assert not fresh and issues
        assert replay.accounting[0].status == "stale"
