"""Typed source joins: list reuse, distinct missing-list errors and declared identifier drops."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Literal

import pytest
from reg_meta.source_evidence import SourceField
from reg_meta_build.source_coding import (
    coding_content_sha256,
    resolve_code_membership,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    value_field,
)
from reg_meta_build.source_support import SourceSupportBindings
from reg_meta_build.source_value_bindings import (
    bind_code_lists,
    bind_occurrence_code_lists,
    open_value_bindings,
)
from reg_meta_build.source_value_periods import value_window
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueValidity,
    SourceValueWindow,
)
from reg_meta_build.sources.scb_auxiliary import (
    clean_identifier_row,
    scb_support_joins,
)

if TYPE_CHECKING:
    from pathlib import Path
from _source_value_bindings_support import (
    join_bindings as _join,
    prepare_value_sources as _prepare,
    value_record as _record,
    value_revision as _revision,
)


@pytest.mark.parametrize(
    "missing,window,expected",
    [
        ("unknown", None, "unknown_code_validity"),
        ("unrestricted", SourceValueWindow("unknown"), "unknown_code_validity"),
        ("unrestricted", None, None),
    ],
)
def test_missing_file_unknown_window_and_unlisted_item_are_distinct(
    tmp_path: Path,
    missing: Literal["unknown", "unrestricted"],
    window: SourceValueWindow | None,
    expected: str | None,
) -> None:
    validity = (
        (SourceValueValidity(2, "1", "bad", "bad", "validity", window=window),)
        if window is not None
        else ()
    )
    source = _prepare(
        tmp_path / "values",
        join=_join(missing=missing),
        validity=validity,
        validity_present=missing != "unknown",
    )
    with open_value_bindings((source,)) as sessions:
        result = bind_code_lists(_record(), sessions)
    assert [issue.code for issue in result.issues] == ([expected] if expected else [])
    resolved = resolve_code_membership(result.claims)
    assert (resolved.segments[0].code_set is None) == (expected is not None)


@pytest.mark.parametrize(
    "identifier",
    (value_field(False), SourceField(status="unknown"), None),
)
def test_unresolved_non_identifier_list_keeps_both_errors(
    tmp_path: Path, identifier: SourceField | None
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(missing="unknown"),
        validity_present=False,
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(identifier=identifier), sessions)
    assert len(bound.claims) == 1
    assert [issue.code for issue in bound.issues] == ["unknown_code_validity"]
    assert [issue.code for issue in resolve_code_membership(bound.claims).issues] == [
        "unknown_code_membership"
    ]


def test_declared_identifier_drops_unresolvable_list_but_keeps_clean_list(
    tmp_path: Path,
) -> None:
    unresolved = _prepare(
        tmp_path / "unresolved",
        join=_join(missing="unknown"),
        validity_present=False,
    )
    clean = _prepare(tmp_path / "clean", join=_join())
    record = _record(identifier=value_field(True))
    with open_value_bindings((unresolved, clean)) as sessions:
        bound = bind_code_lists(record, sessions)
    assert len(bound.claims) == 1
    assert bound.issues == ()
    assert len(bound.bindings) == 2
    assert {binding.claim_id for binding in bound.bindings} == {
        None,
        bound.claims[0].claim_id,
    }
    resolved = resolve_code_membership(bound.claims)
    assert resolved.issues == ()
    assert resolved.segments[0].code_set is not None
    assert resolved.segments[0].code_set.members == (("01", "One"),)


def test_identifierare_join_drops_unresolvable_scb_record_coding(
    tmp_path: Path,
) -> None:
    record = _record(source="scb-registerinformation", provider="scb")
    assert record.fields.identifier is None
    cells = {
        "VarID": (True, "3", "3"),
        "Variabelnamn": (True, "Variable", "Variable"),
        "Variabeldefinition": (True, "Definition", "Definition"),
    }
    identifier = clean_identifier_row(
        tuple(cells), 2, cells, _revision("scb-identifierare")
    )
    support = SourceSupportBindings(
        scb_support_joins(
            {
                "Registerinformation.csv": "scb-registerinformation",
                "Identifierare.csv": "scb-identifierare",
            }
        ),
        (identifier,),
    )
    support.observe(record)
    support.seal()
    assert support.bind(record)[0].fields.identifier == value_field(True)
    source = _prepare(
        tmp_path / "values",
        join=_join(source="scb-registerinformation", missing="unknown"),
        validity_present=False,
    )
    with open_value_bindings((source,)) as sessions:
        unjoined = bind_occurrence_code_lists(source_occurrence(record), sessions)
        joined = bind_occurrence_code_lists(
            source_occurrence(record), sessions, support=support
        )
    assert len(unjoined.claims) == 1
    assert [issue.code for issue in unjoined.issues] == ["unknown_code_validity"]
    assert [
        issue.code for issue in resolve_code_membership(unjoined.claims).issues
    ] == ["unknown_code_membership"]
    assert joined.claims == joined.issues == ()
    assert joined.bindings[0].claim_id is None


@pytest.mark.parametrize("code,label", ((None, "One"), ("01", None)))
def test_declared_identifier_drops_unknown_membership(
    tmp_path: Path, code: str | None, label: str | None
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        values=(SourceValue("a", code, label),),
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(identifier=value_field(True)), sessions)
    assert bound.claims == bound.issues == ()
    assert bound.bindings[0].claim_id is None


def test_declared_identifier_drops_only_bad_membership_period(
    tmp_path: Path,
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        values=(SourceValue("a", "01", "One"), SourceValue("b", None, "Bad")),
        rows=(
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
            SourceValueAssociation(
                3,
                "list",
                "b",
                "values",
                member_id="1001",
                item_id="2",
                supplied_window=value_window("2020-07-01", "2020-07-31"),
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(identifier=value_field(True)), sessions)
    assert len(bound.claims) == 1
    assert bound.issues == ()
    assert bound.claims[0].drop_unknown_membership
    assert coding_content_sha256(bound.claims[0]) is None
    resolved = resolve_code_membership(bound.claims)
    assert resolved.issues == ()
    assert [
        (
            segment.valid_from,
            segment.valid_to,
            segment.code_set.members if segment.code_set else None,
        )
        for segment in resolved.segments
    ] == [
        ("2020-01-01", "2020-06-30", (("01", "One"),)),
        ("2020-07-01", "2020-07-31", None),
        ("2020-08-01", "2020-12-31", (("01", "One"),)),
    ]


def test_declared_identifier_drops_only_bad_validity_period(tmp_path: Path) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(missing="unknown"),
        rows=(
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
            SourceValueAssociation(
                3,
                "list",
                "b",
                "values",
                member_id="1001",
                item_id="2",
                supplied_window=value_window("2020-07-01", "2020-07-31"),
            ),
        ),
        validity=(
            SourceValueValidity(
                2, "1", None, None, "validity", window=SourceValueWindow("known")
            ),
        ),
    )
    with open_value_bindings((source,)) as sessions:
        bound = bind_code_lists(_record(identifier=value_field(True)), sessions)
    assert len(bound.claims) == 1
    assert bound.issues == ()
    resolved = resolve_code_membership(bound.claims)
    assert resolved.issues == ()
    assert [
        (
            segment.valid_from,
            segment.valid_to,
            segment.code_set.members if segment.code_set else None,
        )
        for segment in resolved.segments
    ] == [
        ("2020-01-01", "2020-06-30", (("01", "One"),)),
        ("2020-07-01", "2020-07-31", None),
        ("2020-08-01", "2020-12-31", (("01", "One"),)),
    ]


def test_mixed_identifier_declarations_affect_only_their_own_records(
    tmp_path: Path,
) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(missing="unknown"),
        validity_present=False,
    )
    with open_value_bindings((source,)) as sessions:
        declared = bind_code_lists(
            _record(member=1001, identifier=value_field(True)), sessions
        )
        undeclared = bind_code_lists(
            _record(member=1001, identifier=value_field(False)), sessions
        )
    assert declared.claims == declared.issues == ()
    assert declared.bindings[0].claim_id is None
    assert len(undeclared.claims) == 1
    assert [issue.code for issue in undeclared.issues] == ["unknown_code_validity"]
    assert [
        issue.code for issue in resolve_code_membership(undeclared.claims).issues
    ] == ["unknown_code_membership"]


def test_occurrence_identifier_is_decided_per_source_record(tmp_path: Path) -> None:
    source = _prepare(
        tmp_path / "values",
        join=_join(),
        values=(SourceValue("a", None, "Unknown"),),
        rows=(
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
            SourceValueAssociation(
                3, "list", "a", "values", member_id="1002", item_id="2"
            ),
        ),
    )
    declared = _record(member=1001, identifier=value_field(True))
    other = _record(member=1002, identifier=value_field(False))
    occurrence = replace(source_occurrence(declared), source_records=(declared, other))
    assert occurrence.fields.identifier == value_field(True)
    with open_value_bindings((source,)) as sessions:
        bound = bind_occurrence_code_lists(occurrence, sessions)
    assert len(bound.claims) == 1
    assert [issue.code for issue in resolve_code_membership(bound.claims).issues] == [
        "unknown_code_membership"
    ]
    assert {binding.record_id: binding.claim_id for binding in bound.bindings} == {
        declared.record_id: None,
        other.record_id: bound.claims[0].claim_id,
    }
