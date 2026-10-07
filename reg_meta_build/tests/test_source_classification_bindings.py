"""Checked codebook bindings preserve original evidence and bounded ambiguity: case bindings, sentinels, omissions and plural books."""

import sqlite3
from dataclasses import replace

import pytest
from _source_classification_bindings_support import (
    apply_bindings as _apply,
    binding_declaration as _declaration,
    binding_setup as _setup,
    declared_bindings as _declared,
    form_bindings as _form,
    sole_classification as _sole_classification,
    sole_conformance as _sole_conformance,
)
from catalog_manifest import synthetic_manifest
from reg_meta_build.curation_compile import SentinelCode
from reg_meta_build.db import SCHEMA_VERSION
from reg_meta_build.resolved_catalog import (
    ResolvedClassificationCode,
    write_resolved_catalog,
)
from reg_meta_build.source_classification_bindings import (
    apply_classification_cases,
)
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
from reg_meta_build.source_coding_choices import coding_expectations
from reg_meta_build.source_curation import (
    ClassificationDecision,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    TemporalScope,
    value_field,
)


def test_checked_classification_forms_and_writes_without_copying_canonical_labels(
    tmp_path,
):
    setup = _setup()
    result = _apply(setup)
    assert result.diagnostics == ()
    variable = _form(setup, result)
    state = variable.states[0]
    assert _sole_classification(state) == "fixture"
    assert state.value_set is not None and state.value_set.members == (
        ("01", "Source label"),
    )
    assert (
        _sole_conformance(state) is not None
        and _sole_conformance(state).status == "conforming"
    )
    assert state.provenance is not None and "binding:" in state.provenance
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest=synthetic_manifest(),
        classifications=tuple(setup[3].values()),
    )
    with pytest.raises(ValueError, match="one application"):
        _apply(setup, coding=result.coding)


def test_codebook_change_is_checked_against_current_delivery():
    setup = _setup()
    changed = _setup(code="02")[2]
    assert _apply(setup, coding=changed).diagnostics == ()
    book = setup[3]["fixture"]
    reordered = book.model_copy(update={"codes": tuple(reversed(book.codes))})
    assert _apply(setup, classifications={"fixture": reordered}).diagnostics == ()
    changed_book = book.model_copy(update={"name": "Changed definition"})
    result = _apply(setup, classifications={"fixture": changed_book})
    assert result.diagnostics == ()
    assert (
        _sole_classification(next(iter(result.coding.values())).segments[0])
        == "fixture"
    )


def test_book_losing_the_observed_code_severs_current_binding():
    setup = _setup(code="02")
    assert _apply(setup).diagnostics == ()
    book = setup[3]["fixture"]
    shrunk = book.model_copy(
        update={"codes": tuple(c for c in book.codes if c.code != "02")}
    )
    result = _apply(setup, classifications={"fixture": shrunk})
    assert result.diagnostics[0].code == "nonconforming_classification_codes"
    state = _form(setup, result).states[0]
    assert _sole_classification(state) == "fixture"
    assert (
        _sole_conformance(state) is not None
        and _sole_conformance(state).status == "extended"
    )
    assert state.value_set is not None and state.value_set.members == (
        ("02", "Source label"),
    )


def test_multiple_books_retain_independent_overlap_links_and_inline_coding():
    setup = _setup()
    case = setup[1]
    assert isinstance(case.decision, ClassificationDecision)
    alternate = setup[3]["fixture"].model_copy(update={"slug": "alternate"})
    competing = case.model_copy(
        update={
            "case_id": "competing",
            "decision": case.decision.model_copy(
                update={
                    "classification": "alternate",
                    "expected_classification": "0" * 64,
                    "valid_from": "2020-07-01",
                    "expected_codings": coding_expectations(
                        setup[2][case.decision.column_key].claims,
                        "2020-07-01",
                        "2020-12-31",
                    ),
                }
            ),
        }
    )
    books = {**setup[3], "alternate": alternate}
    result = _apply(setup, (case, competing), classifications=books)
    states = _form(setup, result).states
    assert [
        (
            s.valid_from,
            s.valid_to,
            tuple(link.classification for link in s.classification_links),
        )
        for s in states
    ] == [
        ("2020-01-01", "2020-06-30", ("fixture",)),
        ("2020-07-01", "2020-12-31", ("alternate", "fixture")),
    ]
    assert states[0].value_set == states[1].value_set
    assert result.diagnostics[0].code == "multiple_classifications_declared"
    assert result.diagnostics[0].severity == "warning"
    assert _apply(setup, (competing, case), classifications=books) == result


def test_declared_reference_can_exist_without_inline_codes_but_inline_override_cannot():
    setup = _setup(inline=False)
    declared = _form(setup, _apply(setup)).states[0]
    assert _sole_classification(declared) == "fixture" and declared.value_set is None
    assert _sole_conformance(declared) is None
    case = setup[1]
    inline_case = case.model_copy(
        update={
            "decision": case.decision.model_copy(
                update={"binding_scope": "inline_coding"}
            )
        }
    )
    inline = _apply(setup, (inline_case,))
    assert inline.coding == setup[2] and inline.diagnostics == ()


def test_noncanonical_codes_keep_source_members_and_declared_evidence(tmp_path):
    setup = _setup(code="99")
    result = _apply(setup)
    variable = _form(setup, result)
    state = variable.states[0]
    assert _sole_classification(state) == "fixture" and state.value_set is not None
    assert state.value_set.members == (("99", "Source label"),)
    assert _sole_conformance(state) is not None
    assert _sole_conformance(state).declared_classification == "fixture"
    assert _sole_conformance(state).status == "extended"
    assert result.diagnostics[0].code == "nonconforming_classification_codes"
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest=synthetic_manifest(),
        diagnostic=True,
        classifications=tuple(setup[3].values()),
    )


def test_curated_sentinel_keeps_checked_binding_with_warning(tmp_path):
    setup = _setup(
        code="99",
        sentinels=(SentinelCode(code="99", meaning="not applicable"),),
    )
    sentinel_book = setup[3]["fixture"]
    result = _apply(setup)
    assert [d.code for d in result.diagnostics] == ["sentinel_classification_codes"]
    assert result.diagnostics[0].severity == "warning"
    variable = _form(setup, result)
    state = variable.states[0]
    assert _sole_classification(state) == "fixture"
    assert state.value_set is not None and state.value_set.members == (
        ("99", "Source label"),
    )
    assert _sole_conformance(state) is not None
    assert _sole_conformance(state).status == "extended"
    assert _sole_conformance(state).nonconforming_members == ()
    assert _sole_conformance(state).sentinel_members == (("99", "Source label"),)
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest=synthetic_manifest(),
        classifications=(sentinel_book,),
    )


def test_naming_a_sentinel_does_not_stale_the_binding():
    setup = _setup()
    book = setup[3]["fixture"]
    assert _apply(setup).diagnostics == ()
    sentinel_book = book.model_copy(
        update={"sentinel_codes": (SentinelCode(code="99", meaning="not applicable"),)}
    )
    assert _apply(setup, classifications={"fixture": sentinel_book}).diagnostics == ()


def test_accepted_omission_survives_classification_application():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    omitted = replace(
        original,
        segments=tuple(replace(s, state_disposition="omit") for s in original.segments),
    )
    result = _apply(setup, coding={key: omitted})
    assert result.coding == {key: omitted} and result.diagnostics == ()


def test_declared_classification_does_not_hide_an_uncovered_coding_period():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    claim = replace(
        original.claims[0],
        scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2020-01-01", end="2020-06-30"),),
        ),
    )
    case = setup[1].model_copy(
        update={
            "decision": setup[1].decision.model_copy(
                update={
                    "expected_codings": coding_expectations(
                        (claim,), "2020-01-01", "2020-12-31"
                    )
                }
            )
        }
    )
    result = _apply(setup, (case,), coding={key: resolve_code_membership((claim,))})
    issue = result.coding[key].issues[-1]
    assert (issue.code, issue.valid_from, issue.valid_to) == (
        "missing_coding_period",
        "2020-07-01",
        "2020-12-31",
    )
    assert result.coding[key].segments[-1].code_set is None
    assert result.coding[key].claims == (claim,)


def test_missing_conversion_is_fatal_and_original_membership_change_is_stale():
    setup = _setup()
    with pytest.raises(ValueError, match="unconverted canonical"):
        _apply(setup, classifications={})
    with pytest.raises(ValueError, match="unconverted column"):
        _apply(setup, coding={})
    result = apply_classification_cases(
        (), (setup[1],), coding=setup[2], classifications=setup[3]
    )
    assert result.coding == setup[2]
    assert result.evaluations[0].status == "stale"
    assert result.diagnostics


def test_dated_classification_decision_does_not_date_independent_delivery():
    setup = _setup()
    coding = {
        key: replace(
            resolution,
            segments=tuple(
                replace(
                    segment,
                    period_scope="year_independent",
                    valid_from=None,
                    valid_to=None,
                )
                for segment in resolution.segments
            ),
        )
        for key, resolution in setup[2].items()
    }
    result = _apply(setup, coding=coding)
    assert any(
        issue.code == "unsupported_classification_scope" for issue in result.diagnostics
    )
    assert result.coding == coding


def test_two_book_conformance_and_extensions_are_stored_independently(tmp_path):
    setup = _setup()
    second = setup[3]["fixture"].model_copy(
        update={
            "slug": "second",
            "short_name": "TWO",
            "codes": (ResolvedClassificationCode(code="02", label="Two"),),
        }
    )
    result = _declared(
        setup,
        (_declaration(setup), _declaration(setup, value_field("TWO"))),
        references={"FIX": "fixture", "TWO": "second"},
        classifications={**setup[3], "second": second},
    )
    variable = _form(setup, result)
    state = variable.states[0]
    assert state.value_set.members == (("01", "Source label"),)
    assert [
        (link.classification, link.conformance.status)
        for link in state.classification_links
    ] == [("fixture", "conforming"), ("second", "extended")]
    assert all(link.provenance for link in state.classification_links)
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (variable,),
        output,
        manifest=synthetic_manifest(),
        classifications=(*setup[3].values(), second),
    )
    with sqlite3.connect(output) as connection:
        assert connection.execute(
            "SELECT c.slug, cc.status, cc.overlap FROM classification_conformance cc JOIN classification c ON c.id=cc.declared_classification_id ORDER BY c.slug"
        ).fetchall() == [("fixture", "conforming", 1.0), ("second", "extended", 0.0)]
        assert connection.execute(
            "SELECT c.slug, vc.code FROM classification_conformance_code cc JOIN classification c ON c.id=cc.declared_classification_id JOIN value_code vc USING(code_id)"
        ).fetchall() == [("second", "01")]
        assert (
            connection.execute("SELECT count(*) FROM state_classification").fetchone()[
                0
            ]
            == 2
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert (
            connection.execute(
                "SELECT value FROM import_manifest WHERE key='schema_version'"
            ).fetchone()[0]
            == SCHEMA_VERSION
        )
    from reg_meta.db import SCHEMA_VERSION as READER_SCHEMA_VERSION, open_db
    from reg_meta.errors import RegMetaError

    with open_db(output) as conn:
        assert (
            conn.execute("SELECT count(*) FROM state_classification").fetchone()[0] == 2
        )
    conn.close()
    from reg_meta_build.db import open_built_db

    with open_built_db(output) as conn:
        assert (
            conn.execute("SELECT count(*) FROM state_classification").fetchone()[0] == 2
        )
    conn.close()
    with sqlite3.connect(output) as conn:
        conn.execute(
            "UPDATE import_manifest SET value='6.18.0' WHERE key='schema_version'"
        )
    # The reader and the builder each name the schema version they gate on.
    for opener, version in (
        (open_db, READER_SCHEMA_VERSION),
        (open_built_db, SCHEMA_VERSION),
    ):
        with pytest.raises(RegMetaError) as stale:
            opener(output)
        assert stale.value.code == "schema_incompatible"
        assert "6.18.0" in stale.value.message and version in stale.value.message


@pytest.mark.parametrize("difference", ["code", "label"])
def test_plural_books_do_not_union_contrary_source_domains(difference):
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    first = replace(original.claims[0], version_label="First")
    member = first.members[0]
    changed = (
        replace(member, code="02")
        if difference == "code"
        else replace(member, label="Different meaning")
    )
    second = replace(
        first, claim_id="other", version_label="Second", members=(changed,)
    )
    coding = {key: resolve_code_membership((first, second))}
    alternate = setup[3]["fixture"].model_copy(update={"slug": "alternate"})
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications={**setup[3], "alternate": alternate},
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"First": "fixture", "Second": "alternate"},
    )
    assert result.coding[key].issues == coding[key].issues
    assert coding[key].issues
    assert all(segment.code_set is None for segment in result.coding[key].segments)
    assert all(
        link.conformance is None
        for segment in result.coding[key].segments
        for link in segment.classification_links
    )
    assert tuple(
        link.classification
        for link in result.coding[key].segments[0].classification_links
    ) == ("alternate", "fixture")
