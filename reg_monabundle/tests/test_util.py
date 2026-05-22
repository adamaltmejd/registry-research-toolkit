"""Tests for ``reg_monabundle.runtime._util`` (MONA project-prefix helpers)."""

from __future__ import annotations

from reg_monabundle.runtime._util import (
    lookup_with_prefix_fallback,
    strip_project_prefix,
)


def test_strip_project_prefix_removes_p_number_prefix() -> None:
    assert strip_project_prefix("P1105_LopNr_PersonNr") == "LopNr_PersonNr"
    assert strip_project_prefix("p42_age") == "age"


def test_strip_project_prefix_keeps_non_prefixed_names() -> None:
    assert strip_project_prefix("LopNr_PersonNr") == "LopNr_PersonNr"
    assert strip_project_prefix("Age") == "Age"


def test_lookup_returns_zero_when_value_is_zero() -> None:
    # Regression: `or` short-circuit collapsed 0 to the prefix-stripped lookup.
    d = {"age": 0}
    assert lookup_with_prefix_fallback(d, "Age") == 0


def test_lookup_returns_empty_string_via_prefix_strip() -> None:
    # Regression: `or` short-circuit collapsed "" to a second .get().
    d = {"age": ""}
    assert lookup_with_prefix_fallback(d, "P1105_Age") == ""


def test_lookup_returns_none_when_both_keys_absent() -> None:
    assert lookup_with_prefix_fallback({}, "P1105_Age") is None
    assert lookup_with_prefix_fallback({"other": "x"}, "Age") is None


def test_lookup_returns_list_value_unchanged() -> None:
    d = {"age": [1, 2]}
    assert lookup_with_prefix_fallback(d, "Age") == [1, 2]


def test_lookup_prefers_unstripped_match_over_stripped() -> None:
    # If the unstripped key is present it wins, even when the stripped key
    # also exists with a different value.
    d = {"p1105_age": 1, "age": 2}
    assert lookup_with_prefix_fallback(d, "P1105_Age") == 1


def test_lookup_falls_back_to_stripped_when_unstripped_absent() -> None:
    d = {"age": 7}
    assert lookup_with_prefix_fallback(d, "P1105_Age") == 7
