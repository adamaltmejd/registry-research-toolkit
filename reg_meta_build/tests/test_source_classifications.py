"""Selected canonical codebooks retain literal codes and competing evidence."""

from dataclasses import replace
from pathlib import Path

import pytest
from reg_meta_build.classifications import load_valid_codes
from reg_meta_build.resolved_catalog import ResolvedCodeSet
from reg_meta_build.source_classifications import (
    resolve_canonical_codes,
    resolve_classification_conformance,
)
from reg_meta_build.source_curation import SourceRecordRef
from reg_meta_build.source_records import RecordLocator
from reg_meta_build.source_values import SourceValue


def _value(key: str, code: str | None, label: str | None) -> SourceValue:
    return SourceValue(
        payload_key=key,
        code=code,
        label=label,
        locators=(
            RecordLocator(
                semantic_record_key=("codebook:fixture", key),
                physical_file="codes.csv",
                physical_table="codes.csv",
                physical_record=key,
                physical_cells=(),
            ),
        ),
    )


def test_literal_codes_and_normalized_duplicates_are_order_independent() -> None:
    values = (
        _value("first", "001", "Category"),
        _value("duplicate", "001", "Category"),
        _value("text", "A1", "Other"),
        _value("empty-label", "02", ""),
        _value("non-ascii", "٠١", "Literal"),
    )
    result = resolve_canonical_codes(values, source="fixture", subject="class/fixture")
    assert result == resolve_canonical_codes(
        reversed(values), source="fixture", subject="class/fixture"
    )
    assert result.diagnostics == () and result.source_payloads == 5
    assert [(item.code, item.label, item.level) for item in result.codes] == [
        ("001", "Category", 3),
        ("02", "", 2),
        ("A1", "Other", None),
        ("٠١", "Literal", None),
    ]


def test_conflicting_or_unknown_members_do_not_erase_independent_codes() -> None:
    values = (
        _value("a", "1", "First"),
        _value("b", "1", "Competing"),
        _value("c", "2", None),
        _value("d", None, "Unknown code"),
        _value("e", "03", "Independent"),
    )
    result = resolve_canonical_codes(values, source="fixture", subject="class/fixture")
    assert [item.code for item in result.codes] == ["03"]
    assert {issue.code for issue in result.diagnostics} == {
        "conflicting_classification_labels",
        "unknown_classification_member",
    }
    conflict = next(
        issue
        for issue in result.diagnostics
        if issue.code == "conflicting_classification_labels"
    )
    assert {ref.semantic_record_key for ref in conflict.refs} == {
        ("codebook:fixture", "a"),
        ("codebook:fixture", "b"),
    }
    assert all(
        issue.severity == "error" and issue.withheld_output == ("classification.code",)
        for issue in result.diagnostics
    )
    empty = resolve_canonical_codes(
        values[:-1], source="fixture", subject="class/fixture"
    )
    assert not empty.codes
    assert empty.diagnostics[-1].withheld_output == (
        "classification",
        "classification_bindings",
    )


def test_inconsistent_payload_identity_is_a_contract_error() -> None:
    value = _value("same", "1", "First")
    with pytest.raises(ValueError, match="payload key"):
        resolve_canonical_codes(
            (value, replace(value, label="Different")),
            source="fixture",
            subject="class/fixture",
        )


def _conformance(pairs, canonical):
    return resolve_classification_conformance(
        ResolvedCodeSet(members=tuple(pairs)),
        classification="fixture",
        canonical_codes=frozenset(canonical),
        subject="scb/example/variable",
        refs=(SourceRecordRef(source="fixture", semantic_record_key=("row", "1")),),
        valid_from="2020-01-01",
        valid_to="2020-12-31",
    )


def test_conformance_checks_literal_codes_without_rewriting_source_labels() -> None:
    result = _conformance(
        (("01", "Source wording"), ("01", "Another supplied label"), ("02", "")),
        {"01", "02", "03"},
    )
    assert result.diagnostics == ()
    assert result.conformance.status == "kept"
    assert result.conformance.checked_codes == ("01", "02")
    assert result.conformance.nonconforming_members == ()


@pytest.mark.parametrize("outside", ["", "9", "99", "?", "1"])
def test_noncanonical_tokens_are_not_guessed_to_be_sentinels(outside: str) -> None:
    result = _conformance((("01", "Agreed"), (outside, "Literal source label")), {"01"})
    assert result.conformance.status == "severed"
    assert result.conformance.declared_classification == "fixture"
    assert result.conformance.nonconforming_members == (
        (outside, "Literal source label"),
    )
    issue = result.diagnostics[0]
    assert issue.code == "nonconforming_classification_codes"
    assert issue.severity == "error" and issue.withheld_output == (
        "state.classification",
    )
    assert issue.refs[0].semantic_record_key == ("row", "1")
    assert (issue.valid_from, issue.valid_to) == ("2020-01-01", "2020-12-31")


def test_high_overlap_does_not_waive_one_unexplained_code() -> None:
    pairs = tuple((str(i), f"Label {i}") for i in range(100))
    result = _conformance(pairs, {str(i) for i in range(99)})
    assert result.conformance.status == "severed"
    assert result.conformance.nonconforming_members == (("99", "Label 99"),)
    assert result == _conformance(tuple(reversed(pairs)), {str(i) for i in range(99)})


def test_sektorkod_cohort_conforms_exactly_and_extras_stay_severed() -> None:
    """Y-170 scope against the SHIPPED books: the observed 11-code cohort is
    kept under SEKTORKOD, severed against SEKTOR2000 (code 15 absent there is
    the invariant — an accepted binding alone cannot make it canonical), and
    pre-2000 extras plus sentinel spellings stay severed under SEKTORKOD.
    Adding 15 to sektor2000.csv turns this test red."""
    books = Path(__file__).resolve().parent.parent / "input_data" / "classifications"
    sektorkod = set(load_valid_codes(books / "sektorkod.csv"))
    insekt = set(load_valid_codes(books / "sektor2000.csv"))
    assert sektorkod == {
        "00",
        "11",
        "12",
        "13",
        "14",
        "15",
        "21",
        "22",
        "23",
        "24",
        "25",
    }
    # The invariant: INSEKT's Undersektor level literally reuses 11-14/21-25
    # with different meanings, but 15 and 00 are absent — so the cohort severs.
    assert "15" not in insekt
    assert "00" not in insekt
    observed = tuple((code, f"Source {code}") for code in sorted(sektorkod))
    kept = _conformance(observed, sektorkod)
    assert kept.conformance.status == "kept"
    assert kept.conformance.nonconforming_members == ()
    assert kept.diagnostics == ()
    misbound = _conformance(observed, insekt)
    assert misbound.conformance.status == "severed"
    assert misbound.conformance.declared_classification == "fixture"
    assert [code for code, _ in misbound.conformance.nonconforming_members] == [
        "00",
        "15",
    ]
    # Extras beyond the book (pre-2000 codes, sentinel spellings) are listed
    # member-by-member; the conforming remainder does not absorb them.
    extras = observed + (
        ("19", "Source 19"),
        ("99", "Source 99"),
        ("0", "Source 0"),
        ("000", "Source 000"),
        ("", "Source empty"),
    )
    severed = _conformance(extras, sektorkod)
    assert severed.conformance.status == "severed"
    assert severed.conformance.checked_codes == tuple(
        sorted(sektorkod | {"19", "99", "0", "000", ""})
    )
    assert severed.conformance.nonconforming_members == (
        ("", "Source empty"),
        ("0", "Source 0"),
        ("000", "Source 000"),
        ("19", "Source 19"),
        ("99", "Source 99"),
    )
    assert severed.diagnostics[0].code == "nonconforming_classification_codes"


def test_missing_canonical_conversion_is_fatal_not_a_curation_issue() -> None:
    with pytest.raises(ValueError, match="nonempty codebook"):
        _conformance((("01", "Label"),), set())
