"""Coding certificates: label equivalence, closed-alpha own names, label witnesses, decimal-comma and period blocks."""

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
from pydantic import ValidationError
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
)
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_curation import (
    SourceEvidence,
)
from reg_meta_build.source_records import (
    ScopeInterval,
    TemporalScope,
    value_field,
)


@pytest.mark.parametrize(
    "labels,selected",
    [
        (["Region", "Region"], "Region"),
        (["Region"], "Region"),
        (["Landsting", "Region"], "Other"),
    ],
)
def test_label_equivalence_requires_exact_positive_source_alternatives(
    labels, selected
):
    from reg_meta_build.source_curation import CodeLabelEquivalence

    with pytest.raises(ValidationError):
        CodeLabelEquivalence(code="14", labels=labels, selected_label=selected)


@pytest.mark.parametrize("three_codes", [False, True])
def test_closed_alpha_own_name_certifies_bare_source_codes(three_codes):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import SourceEnumeration

    pairs = (("LEG", "legitimation"), ("SPEC", "specialitet"))
    if three_codes:
        pairs += (("EXAM", "examen"),)
    lines = tuple(f"{code} = {label}" for code, label in pairs)
    prose = "Kategori " + ", ".join(lines[:-1]) + " eller " + lines[-1]
    record = _record().model_copy(
        update={
            "fields": _record().fields.model_copy(update={"name": value_field(prose)})
        }
    )
    claims = (
        CodeListClaim(
            "bare",
            record.edition_scope,
            tuple(
                CodeMembershipClaim(code, None, TemporalScope(kind="year_independent"))
                for code, _ in pairs
            ),
        ),
    )
    authority = _row_authority(record, claims).model_dump(mode="json")
    authority.update(
        enumeration={
            "field": "name",
            "syntax": "kategori-alpha-equals",
            "lines": lines,
        },
        raw_codings=[coding_source_sha256(claims[0])],
    )
    cases, issues, register, scope, columns, column = _compile_entry(
        "documented",
        {
            "members": pairs,
            "version_label": "Own literal meanings",
            "source_authority": authority,
        },
        claims,
        record=record,
    )
    assert len(cases) == 1 and not issues
    evidence = SourceEvidence((record,), value_bindings={})
    applied = apply_coding_choices(evidence, cases, coding={column: claims})
    assert not applied.diagnostics
    assert set(applied.coding[column].segments[0].code_set.members) == set(pairs)
    assert applied.coding[column].claims == claims
    certificate = SourceEnumeration.model_validate(authority["enumeration"])
    for changed in (
        "Not " + prose,
        prose + " or OTHER = other",
        prose.replace("legitimation", "unknown"),
        prose.replace("SPEC", "OTHER"),
    ):
        fields = record.fields.model_copy(update={"name": value_field(changed)})
        assert not certificate.matches_fields(fields)
        altered = record.model_copy(update={"fields": fields})
        fresh, issues = compile_coding_register(
            register,
            scope,
            originals=(altered,),
            columns={column: (altered,)},
            column_scopes=_column_scopes(columns),
            coding={column: claims},
        )
        assert not fresh and issues
        assert apply_coding_choices(
            SourceEvidence((altered,), value_bindings={}),
            cases,
            coding={column: claims},
        ).diagnostics
    changed_claim = replace(
        claims[0],
        members=claims[0].members
        + (CodeMembershipClaim("OTHER", None, TemporalScope(kind="year_independent")),),
    )
    assert apply_coding_choices(
        evidence, cases, coding={column: (changed_claim,)}
    ).diagnostics


@pytest.mark.parametrize("drift", ["missing", "peer", "parent", "code_label"])
def test_closed_alpha_certificate_refuses_complete_source_evidence_drift(drift):
    from reg_meta_build.source_coding import coding_source_sha256

    record = _record().model_copy(
        update={
            "fields": _record().fields.model_copy(
                update={
                    "name": value_field(
                        "Kategori LEG = legitimation eller SPEC = specialitet"
                    )
                }
            )
        }
    )
    claims = (
        CodeListClaim(
            "bare",
            record.edition_scope,
            tuple(
                CodeMembershipClaim(code, None, TemporalScope(kind="year_independent"))
                for code in ("LEG", "SPEC")
            ),
        ),
    )
    authority = _row_authority(record, claims).model_dump(mode="json")
    authority.update(
        enumeration={
            "field": "name",
            "syntax": "kategori-alpha-equals",
            "lines": ["LEG = legitimation", "SPEC = specialitet"],
        },
        raw_codings=[coding_source_sha256(claims[0])],
    )
    cases, issues, register, scope, columns, column = _compile_entry(
        "documented",
        {
            "members": [["LEG", "legitimation"], ["SPEC", "specialitet"]],
            "version_label": "Own literal meanings",
            "source_authority": authority,
        },
        claims,
        record=record,
    )
    assert len(cases) == 1 and not issues
    rows = (record,)
    if drift == "missing":
        rows = ()
    elif drift == "peer":
        rows += (_record(2021),)
    elif drift == "parent":
        rows = (record.model_copy(update={"parent_facts": ()}),)
    else:
        claims = (
            replace(
                claims[0],
                members=(
                    replace(claims[0].members[0], label="Unreviewed"),
                    claims[0].members[1],
                ),
            ),
        )
    fresh, issues = compile_coding_register(
        register,
        scope,
        originals=rows,
        columns={column: rows},
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert not fresh and issues
    assert apply_coding_choices(
        SourceEvidence(rows, value_bindings={}), cases, coding={column: claims}
    ).diagnostics


@pytest.mark.parametrize(
    "drift", [None, "label", "code", "missing", "outside", "wrong", "sibling"]
)
def test_documented_label_witness_preserves_period_specific_missing_tokens(drift):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import CodeLabelEquivalence

    record = _record()
    base = replace(
        _claim("Status", "30", "2016-01-01", "2016-12-31"),
        members=tuple(
            CodeMembershipClaim(code, label, TemporalScope(kind="year_independent"))
            for code, label in (
                ("30", "Matrix imputation"),
                ("30", "Model imputation"),
                ("6", "Missing"),
            )
        ),
    )
    future = replace(
        base,
        claim_id="future",
        scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2017-01-01", end="2017-12-31"),),
        ),
        members=(
            *base.members,
            CodeMembershipClaim(
                "NULL", "Missing", TemporalScope(kind="year_independent")
            ),
        ),
    )
    claims = (base, future)
    authority = _row_authority(record, claims).model_copy(
        update={
            "raw_codings": [coding_source_sha256(c) for c in claims],
            "label_equivalences": [
                CodeLabelEquivalence(
                    code="30",
                    labels=("Matrix imputation", "Model imputation"),
                    selected_label="Model imputation",
                )
            ],
            "witness": ("2016-01-01", "2016-12-31"),
        }
    )
    values = {
        "members": [["30", "Model imputation"], ["6", "Missing"]],
        "version_label": "Status",
        "source_authority": authority,
    }
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented", values, claims, record=record
    )
    assert not diagnostics
    changed = claims
    if drift == "label":
        changed = (
            replace(
                base,
                members=(
                    replace(base.members[0], label="Other method"),
                    *base.members[1:],
                ),
            ),
            future,
        )
    elif drift == "code":
        changed = (
            replace(base, members=(*base.members, replace(base.members[2], code="7"))),
            future,
        )
    elif drift == "missing":
        changed = (future,)
    elif drift == "sibling":
        changed = (
            base,
            replace(
                future,
                members=(*future.members, replace(future.members[-1], code="NEW")),
            ),
        )
    elif drift == "wrong":
        values = {
            **values,
            "source_authority": authority.model_copy(
                update={"witness": ("2017-01-01", "2017-12-31")}
            ),
        }
        assert _compile_entry("documented", values, claims, record=record)[1]
        return
    elif drift == "outside":
        values = {
            **values,
            "source_authority": authority.model_copy(
                update={"witness": ("2015-01-01", "2015-12-31")}
            ),
        }
        assert _compile_entry("documented", values, claims, record=record)[1]
        return
    result = apply_coding_choices((record,), cases, coding={column: changed})
    assert result.coding[column].claims == changed
    if drift is None:
        selected = next(
            s for s in result.coding[column].segments if s.valid_from == "2020-01-01"
        )
        assert set(selected.code_set.members) == {
            ("30", "Model imputation"),
            ("6", "Missing"),
        }
        # A later witnessed domain keeps its actual missing token.
        later = {
            **values,
            "members": [*values["members"], ["NULL", "Missing"]],
            "source_authority": authority.model_copy(
                update={"witness": ("2017-01-01", "2017-12-31")}
            ),
        }
        assert not _compile_entry("documented", later, claims, record=record)[1]
    else:
        assert result.accounting[0].status == "stale"
        assert _compile_entry("documented", values, changed, record=record)[1]


@pytest.mark.parametrize("contrary", [None, "code", "label"])
def test_documented_witness_refuses_contrary_complete_target_domain(contrary):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import CodeLabelEquivalence

    record = _record()
    members = tuple(
        CodeMembershipClaim(code, label, TemporalScope(kind="year_independent"))
        for code, label in (("30", "Matrix"), ("30", "Model"), ("6", "Missing"))
    )
    witness = replace(
        _claim("Status", "30", "2016-01-01", "2016-12-31"), members=members
    )
    target_members = (members[1], members[2])
    if contrary == "code":
        target_members = (*target_members, replace(members[2], code="7"))
    elif contrary == "label":
        target_members = (replace(members[1], label="Directly observed"), members[2])
    target = replace(_claim("Status", "30"), claim_id="target", members=target_members)
    claims = (witness, target)
    authority = _row_authority(record, claims).model_copy(
        update={
            "raw_codings": [coding_source_sha256(c) for c in claims],
            "witness": ("2016-01-01", "2016-12-31"),
            "label_equivalences": [
                CodeLabelEquivalence(
                    code="30", labels=("Matrix", "Model"), selected_label="Model"
                )
            ],
        }
    )
    values = {
        "members": [["30", "Model"], ["6", "Missing"]],
        "version_label": "Status",
        "source_authority": authority,
    }
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented", values, claims, record=record
    )
    if contrary:
        assert diagnostics and not cases
    else:
        assert not diagnostics
        result = apply_coding_choices((record,), cases, coding={column: claims})
        assert not result.diagnostics and result.coding[column].claims == claims
        # Ordinary documentary entries still cannot overwrite a complete source list.
        values["source_authority"] = authority.model_copy(update={"witness": None})
        assert _compile_entry("documented", values, claims, record=record)[1]


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "new_anchor",
        "changed_period",
        "missing_row",
        "foreign_sheet",
        "multiplicity",
        "out_of_block",
        "outside",
        "multiple_claims",
    ],
)
def test_documented_period_block_retains_literal_codes_and_all_raw_claims(drift):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_values import SourceValueAssociation, SourceValueWindow

    record = _record()
    early = TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start="2019-01-01", end="2020-12-31"),),
    )
    later = TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start="2021-01-01", end="2021-12-31"),),
    )
    unrestricted = TemporalScope(kind="year_independent")
    pairs = (
        ("00", "Original"),
        ("2-3", "Range token"),
        ("blank", "No type"),
        ("00", "Later meaning"),
        ("blank", "Later missing meaning"),
    )
    members = []
    for row, (code, label) in enumerate(pairs, 1):
        period = early if row == 1 else later if row == 4 else None
        association = SourceValueAssociation(
            row,
            "book",
            str(row),
            "book.xlsx",
            "codes",
            supplied_period="2019-2020" if row == 1 else "2021" if row == 4 else None,
            supplied_window=SourceValueWindow(
                "known",
                period.intervals[0].start,
                period.intervals[0].end,
            )
            if period is not None
            else None,
        )
        members.append(
            CodeMembershipClaim(
                code, label, period or unrestricted, associations=(association,)
            )
        )
    claim = replace(
        _claim("Source", "00", "2019-01-01", "2021-12-31"), members=tuple(members)
    )
    claims = (claim,)
    authority = _row_authority(record, claims).model_copy(
        update={
            "raw_codings": [coding_source_sha256(claim)],
            "period_block": members[0].associations[0].locator,
        }
    )
    values = {
        "members": list(pairs[:3]),
        "version_label": "Reviewed2019-2020block",
        "data_warning": "The source tokens' stored encoding is unverified.",
        "source_authority": authority,
    }
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented",
        values,
        claims,
        record=record,
    )
    assert not diagnostics and cases
    assert cases[0].decision.data_warning == values["data_warning"]
    changed = members.copy()
    if drift in {"new_anchor", "changed_period", "foreign_sheet"}:
        index = 0 if drift == "changed_period" else 1
        association = changed[index].associations[0]
        update = (
            {"source_table": "other"}
            if drift == "foreign_sheet"
            else {
                "supplied_period": "2018-2020",
                "supplied_window": SourceValueWindow(
                    "known", "2018-01-01", "2020-12-31"
                ),
            }
        )
        changed[index] = replace(
            changed[index], associations=(replace(association, **update),)
        )
    elif drift == "missing_row":
        changed.pop(2)
    elif drift == "multiplicity":
        changed[1] = replace(changed[1], associations=changed[1].associations * 2)
    elif drift == "out_of_block":
        changed[-1] = replace(changed[-1], label="Contrary later source meaning")
    elif drift == "outside":
        values = {**values, "periods": [["2021-01-01", "2021-12-31"]]}
        assert _compile_entry("documented", values, claims, record=record)[1]
        return
    fresh = (replace(claim, members=tuple(changed)),)
    if drift == "multiple_claims":
        fresh = (*fresh, replace(claim, claim_id="other"))
    result = apply_coding_choices((record,), cases, coding={column: fresh})
    assert result.coding[column].claims == fresh
    if drift is None:
        assert not result.diagnostics
        assert any(
            s.code_set is not None and set(s.code_set.members) == set(pairs[:3])
            for s in result.coding[column].segments
        )
    else:
        assert result.accounting[0].status == "stale"
        assert _compile_entry("documented", values, fresh, record=record)[1]


@pytest.mark.parametrize("end", ["2020-12-31", None])
def test_documented_period_block_intersects_known_delivery_scope(end):
    from reg_meta_build.source_coding_choices import (
        documented_period_block,
        documented_period_block_matches,
    )
    from reg_meta_build.source_values import SourceValueAssociation, SourceValueWindow

    effective = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020-01-01", end=end),)
    )
    association = SourceValueAssociation(
        6,
        "book",
        "6",
        "book.xlsx",
        "codes",
        supplied_period="2019-2020" if end else "2019-",
        supplied_window=SourceValueWindow("known", "2019-01-01", end),
    )
    claim = replace(
        _claim("Source", "00", "2020-01-01", "2021-12-31"),
        scope=effective,
        members=(
            CodeMembershipClaim(
                "00", "Original", effective, associations=(association,)
            ),
            CodeMembershipClaim(
                "blank",
                "Missing",
                TemporalScope(kind="year_independent"),
                associations=(
                    replace(
                        association,
                        row_number=7,
                        value_key="7",
                        supplied_period=None,
                        supplied_window=None,
                    ),
                ),
            ),
        ),
    )
    pairs = (("00", "Original"), ("blank", "Missing"))
    assert documented_period_block((claim,), association.locator) == (effective, pairs)
    assert documented_period_block_matches(
        (claim,), association.locator, pairs, "2020-01-01", "2020-12-31", effective
    )
    assert not documented_period_block_matches(
        (claim,), association.locator, pairs, "2019-01-01", "2019-12-31", effective
    )
    assert claim.members[0].associations[0].supplied_period == (
        "2019-2020" if end else "2019-"
    )
