"""Compact coding choices and own-source certificates keep full runtime guards and refuse evidence drift."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_coding_choices_support import (
    apply_choices as _apply,
    choice_record as _record,
    column_scopes as _column_scopes,
    compile_entry as _compile_entry,
    list_claim as _claim,
    row_authority as _row_authority,
)
from pydantic import ValidationError
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
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


def _compact_choice_fixture():
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256

    records = (_record(), _record(2021))
    claims = (
        _claim("keep", "01"),
        _claim("other", "02"),
        _claim("later", "03", "2021-01-01", "2021-12-31"),
    )
    values = {
        "keep": "keep",
        "over": ["other"],
        "expected_evidence_sha256": acknowledgement_evidence_sha256(
            records, (coding_source_sha256(c) for c in claims)
        ),
    }
    _, _, register, scope, _, column = _compile_entry("choice", values, claims)
    columns = {column: records}
    cases, issues = compile_coding_register(
        register,
        scope,
        originals=records,
        columns=columns,
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert len(cases) == 1 and not issues
    return records, claims, register, scope, column, cases


def test_compact_choice_preserves_full_runtime_original_and_raw_coding_guards():
    records, claims, _, _, column, cases = _compact_choice_fixture()
    assert cases[0].targets == capture_expectations(
        records, fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    evidence = SourceEvidence(
        records, effective_occurrences=tuple(source_occurrence(r) for r in records)
    )
    applied = apply_coding_choices(evidence, cases, coding={column: claims})
    assert applied.accounting[0].status == "applied"
    assert applied.coding[column].claims == claims
    contrary = (*claims[:-1], replace(claims[-1], members=()))
    rejected = apply_coding_choices(evidence, cases, coding={column: contrary})
    assert rejected.accounting[0].status == "stale"


@pytest.mark.parametrize(
    "drift",
    [
        "field",
        "parent",
        "removed",
        "duplicate",
        "coding",
        "outside_coding",
        "duplicate_coding",
    ],
)
def test_compact_choice_rejects_complete_history_evidence_drift(drift):
    records, claims, register, scope, column, _ = _compact_choice_fixture()
    if drift == "field":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"definition": value_field("Changed")}
                    )
                }
            ),
        )
    elif drift == "parent":
        records = (records[0], records[1].model_copy(update={"parent_facts": ()}))
    elif drift == "removed":
        records = records[:1]
    elif drift == "duplicate":
        records += records[:1]
    elif drift == "coding":
        claims = (replace(claims[0], members=()), *claims[1:])
    elif drift == "outside_coding":
        claims = (*claims[:-1], replace(claims[-1], members=()))
    else:
        claims += claims[:1]
    columns = {column: records}
    cases, issues = compile_coding_register(
        register,
        scope,
        originals=records,
        columns=columns,
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert not cases and len(issues) == 1
    assert issues[0].code == "stale_curation_entry"


def test_compact_choice_rejects_digest_with_verbose_authority():
    records, claims, register, _, _, _ = _compact_choice_fixture()
    from reg_meta_build.curation_tree import CodingChoiceEntry

    values = register.coding.choice[0].model_dump()
    values["source_authority"] = _row_authority(records[0], claims)
    with pytest.raises(ValidationError, match="exclusive"):
        CodingChoiceEntry.model_validate(values)


@pytest.mark.parametrize("drift", [None, "code", "label", "ambiguous_label"])
def test_choice_member_digest_matches_verbose_exact_domain(drift):
    from reg_meta.source_evidence import canonical_sha256

    claims = (_claim("list", "01"), _claim("list", "02"))
    values = {"keep": "list", "over": ["list"]}
    verbose, issues, _, _, _, _ = _compile_entry(
        "choice", {**values, "keep_members": [["01", "Label"]]}, claims
    )
    assert len(verbose) == 1 and not issues
    digest = canonical_sha256([["01", "Label"]])
    if drift in {"code", "label"}:
        member = replace(claims[0].members[0], **{drift: "Changed"})
        claims = (replace(claims[0], members=(member,)), claims[1])
    if drift == "ambiguous_label":
        # Without an exact selector, matching the shared label is still ambiguous.
        _, issues, _, _, _, _ = _compile_entry("choice", values, claims)
        assert issues[0].code == "overbroad_curation_entry"
    compact, issues, _, _, _, _ = _compile_entry(
        "choice", {**values, "keep_members_sha256": digest}, claims
    )
    if drift in {"code", "label"}:
        assert not compact and issues[0].code == "stale_curation_entry"
    else:
        assert not issues and compact == verbose


def test_choice_member_digest_rejects_verbose_selector_and_retains_duplicate_claims():
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.curation_tree import CodingChoiceEntry

    claims = (_claim("keep", "01"), _claim("other", "02"))
    digest = canonical_sha256([["01", "Label"]])
    values = {"keep": "keep", "over": ["other"], "keep_members_sha256": digest}
    cases, issues, register, _, _, column = _compile_entry("choice", values, claims)
    assert not issues
    duplicate = replace(claims[0], claim_id="duplicate")
    applied = apply_coding_choices(
        (_record(),), cases, coding={column: (*claims, duplicate)}
    )
    assert applied.accounting[0].status == "applied"
    assert applied.coding[column].claims == (*claims, duplicate)
    entry = register.coding.choice[0].model_dump()
    entry["keep_members"] = [["01", "Label"]]
    with pytest.raises(ValidationError, match="exclusive"):
        CodingChoiceEntry.model_validate(entry)


@pytest.mark.parametrize("drift", [None, "member", "label", "fingerprint", "period"])
def test_complete_own_source_certificate_retains_domain_and_refuses_drift(drift):
    from reg_meta_build.source_coding import coding_source_sha256

    record = _record()
    claims = (_claim("Own list", "01"),)
    authority = _row_authority(record, claims).model_copy(
        update={"raw_codings": [coding_source_sha256(c) for c in claims]}
    )
    values = {
        "version_label": "Own list",
        "members": [["01", "Label"]],
        "source_authority": authority,
    }
    cases, issues, register, scope, _, column = _compile_entry(
        "documented", values, claims
    )
    assert len(cases) == 1 and not issues
    applied = _apply(record, claims, *cases)
    assert applied.accounting[0].status == "applied"
    source = resolve_code_membership(claims)
    assert applied.coding[column].claims == source.claims
    assert [
        (s.valid_from, s.valid_to, s.code_set, s.version_label)
        for s in applied.coding[column].segments
    ] == [
        (s.valid_from, s.valid_to, s.code_set, s.version_label) for s in source.segments
    ]
    if drift is None:
        return
    if drift in {"member", "label"}:
        claims = (
            replace(
                claims[0],
                members=(
                    replace(
                        claims[0].members[0],
                        **{"code" if drift == "member" else "label": "Changed"},
                    ),
                ),
            ),
        )
    elif drift == "fingerprint":
        claims = (replace(claims[0], claim_id="Changed raw assertion"),)
    else:
        entry = register.coding.documented[0].model_copy(
            update={"periods": [["2019-01-01", "2020-12-31"]]}
        )
        register = register.model_copy(
            update={
                "coding": register.coding.model_copy(update={"documented": [entry]})
            }
        )
    columns = {column: (record,)}
    fresh, issues = compile_coding_register(
        register,
        scope,
        originals=(record,),
        columns=columns,
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert not fresh and len(issues) == 1
    assert issues[0].code == "stale_curation_entry"
    if drift != "period":
        rejected = _apply(record, claims, *cases)
        assert rejected.accounting[0].status == "stale"


def test_ordinary_documented_authority_still_refuses_a_complete_existing_list():
    record = _record()
    claims = (_claim("Own list", "01"),)
    cases, issues, *_ = _compile_entry(
        "documented",
        {
            "version_label": "Own list",
            "members": [["01", "Label"]],
            "source_authority": _row_authority(record, claims),
        },
        claims,
    )
    assert not cases and len(issues) == 1
    assert "already has a complete source list" in issues[0].detail


@pytest.mark.parametrize("mixed", ["pdf", "unsupported_enumeration"])
def test_complete_own_source_certificate_rejects_mixed_authority(mixed):
    import json

    from reg_meta_build.curation_tree import CodingDocumentedEntry
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import SourceEnumeration

    record = _record()
    claims = (_claim("Own list", "01"),)
    authority = _row_authority(record, claims).model_copy(
        update={"raw_codings": [coding_source_sha256(c) for c in claims]}
    )
    values = {
        "variable": "1.2",
        "variant": "1.3",
        "column": "A",
        "periods": [["2020-01-01", "2020-12-31"]],
        "reason": "Exact own source certificate",
        "source": "Prepared source",
        "version_label": "Own list",
        "members": [["01", "Label"]],
        "source_authority": authority,
    }
    if mixed == "pdf":
        values["document_url"] = "https://example.test/source.pdf"
    else:
        values["source_authority"] = authority.model_copy(
            update={
                "enumeration": SourceEnumeration(
                    field="definition",
                    syntax="ascii-decimal-dot-space",
                    lines=("01. Label",),
                ),
            }
        )
    with pytest.raises(ValidationError, match="authority|certificate"):
        CodingDocumentedEntry.model_validate_json(
            json.dumps(values, default=lambda v: v.model_dump(mode="json"))
        )


def test_guarded_coding_warning_retains_facts_and_rejects_changed_evidence():
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import (
        SourceWarningDecision,
        acknowledgement_evidence_sha256,
        evaluate_cases,
    )

    record = _record()
    claims = (_claim("LA15", "LA1501"),)
    values = {
        "fields": ["data_type", "coding"],
        "data_warning": "Integer storage and textual response codes disagree.",
        "expected_evidence_sha256": acknowledgement_evidence_sha256(
            (record,), (coding_source_sha256(c) for c in claims)
        ),
    }
    cases, issues, register, scope, columns, column = _compile_entry(
        "warning", values, claims, record=record
    )
    assert not issues and len(cases) == 1
    assert isinstance(cases[0].decision, SourceWarningDecision)
    assert cases[0].targets == capture_expectations(
        (record,), fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    evidence = SourceEvidence(
        (record,), effective_occurrences=(source_occurrence(record),)
    )
    assert evaluate_cases(cases, evidence)[0].status == "applicable"
    # The warning creates no coding assignment, type correction, or omitted state.
    assert cases[0].decision.kind == "source_warning"
    for changed_records, changed_claims in (
        (
            (
                record.model_copy(
                    update={
                        "fields": record.fields.model_copy(
                            update={"data_type": value_field("text")}
                        )
                    }
                ),
            ),
            claims,
        ),
        ((record,), (_claim("LA15", "LA1502"),)),
    ):
        new_cases, diagnostics = compile_coding_register(
            register,
            scope,
            originals=changed_records,
            columns={column: changed_records},
            column_scopes=_column_scopes(columns),
            coding={column: changed_claims},
        )
        assert not new_cases
        assert [(d.code, d.severity) for d in diagnostics] == [
            ("stale_curation_entry", "error")
        ]
