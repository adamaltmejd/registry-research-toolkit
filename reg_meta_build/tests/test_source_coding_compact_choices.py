"""Own-source coding certificates retain the source domain and refuse evidence drift.

Waiting on the build-case `raw_codings` and `source_codings` placeholders: an own
certificate (`[[coding.documented]]` with `source_authority.raw_codings`) cannot be
authored in `cases/build/` until a curator's fingerprints render there. The file's
other claims moved to `cases/build/coding-entries-apply-or-go-stale-per-register` and
`cases/curation_toml/coding-*`, or were dropped (Stage 8a PR body).
"""

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
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_coding import (
    resolve_code_membership,
)


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
