"""Shared definitions group checked partitions without inferring identity."""

from dataclasses import replace

import pytest
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import SourceField, value_field
from reg_meta_build.source_siblings import resolve_sibling_pairs
from test_source_scope import record


def sibling(column, member, *, dtype="integer"):
    row = record(member=member, column=column)
    item = source_occurrence(row)
    return replace(
        item,
        variable_key=(*item.variable_key, column),
        identity_checked=True,
        fields=item.fields.model_copy(update={"data_type": value_field(dtype)}),
    )


def resolve(*items):
    return resolve_sibling_pairs(
        items,
        identities={
            item.variable_key: "scb/test/" + item.fields.column_name.value.lower()
            for item in items
        },
    )


def test_checked_siblings_group_and_reordering_preserves_results():
    a, b = sibling("A", 1), sibling("B", 2)
    result = resolve(a, b)
    assert result.pairs == (("scb/test/a", "scb/test/b"),)
    assert result == resolve(b, a)
    with pytest.raises(ValueError, match="checked identity"):
        resolve(replace(a, identity_checked=False), b)


@pytest.mark.parametrize(
    ("a", "b", "dtype", "kind"),
    [
        ("A", "B", "text", "shape_conflict"),
        ("VALUE", "VALUENAMN", "text", "code_label"),
    ],
)
def test_co_delivery_guards_exclude_distinct_representations(a, b, dtype, kind):
    result = resolve(sibling(a, 1), sibling(b, 2, dtype=dtype))
    assert not result.pairs
    assert result.decisions[0].kind == kind


def test_withheld_shape_does_not_reuse_the_rejected_value():
    a, b = sibling("A", 1), sibling("B", 2)
    result = resolve(a, replace(b, withheld_fields=("data_type",)))
    assert not result.pairs
    assert result.diagnostics[0].code == "ambiguous_sibling_shape"


def test_negative_availability_and_different_editions_are_not_co_delivery():
    a, b = sibling("A", 1), sibling("B", 2)
    absent = replace(
        b,
        fields=b.fields.model_copy(
            update={"availability": SourceField(status="negative")}
        ),
    )
    assert not resolve(a, absent).pairs
    assert not resolve(a, replace(b, edition_key=(*b.edition_key, "other"))).pairs


def test_equal_recency_conflicting_shapes_withhold_the_group():
    a, b = sibling("A", 1), sibling("B", 2)
    b_other = sibling("B", 3, dtype="text")
    result = resolve(a, b, b_other)
    assert not result.pairs
    assert result.decisions[0].kind == "unresolved_shape"
