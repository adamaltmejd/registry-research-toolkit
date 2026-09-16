"""Actual authored TOML/CSV layouts stay source evidence before resolution."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.source_records import SourceRevision
from reg_meta_build.sources.curated_records import (
    CuratedSourceError,
    read_curated_source,
)

if TYPE_CHECKING:
    from pathlib import Path


def _source(
    tmp_path: Path, text: str, *, filename: str = "agency.toml"
) -> tuple[Path, SourceRevision]:
    path = tmp_path / filename
    payload = text.encode()
    path.write_bytes(payload)
    revision = SourceRevision.create(
        dataset=f"authored-{filename}",
        publisher="Agency",
        purpose="source fixture",
        upstream_revision="fixture-1",
        artifact_path=filename,
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )
    return path, revision


_BASE = """
[[register]]
key = "sample-register"
name = "Sample register"
valid_from = "2001-01-01"
purpose = "A declared purpose"
"""
_VARIABLE = """
[[register.variable]]
name = "A variable"
column = "CODE"
data_type = "text"
"""


def test_parent_dates_flags_and_variant_memberships_remain_separate_claims(
    tmp_path: Path,
) -> None:
    path, revision = _source(
        tmp_path,
        _BASE
        + """
[[register.variant]]
key = "first"
name = "First delivery"
valid_from = "2005-01-01"
valid_to = "2009-12-31"
[[register.variant]]
key = "second"
name = "Second delivery"
"""
        + _VARIABLE
        + """
variants = ["first", "second"]
is_identifier = false
is_sensitive = true
classification = "Unresolved declared classification"
""",
    )
    clean = read_curated_source(path, revision, provider="agency")
    assert len(clean.records) == 4
    parent, first, second, variable = clean.records
    assert (
        parent.parent_facts[0].fields.purpose is not None
        and parent.parent_facts[0].fields.purpose.value == "A declared purpose"
    )
    assert (
        parent.parent_facts[0].fields.coverage_from is not None
        and parent.parent_facts[0].fields.coverage_from.value == "2001-01-01"
    )
    assert parent.edition_period_scope.kind == "unknown"
    assert [
        (span.start, span.end) for span in first.edition_period_scope.intervals
    ] == [("2005-01-01", "2009-12-31")]
    assert second.parent_facts[0].fields.coverage_from is None
    assert parent.fields.purpose is None and parent.fields.name is None
    assert parent.parent_field_locators(0, "purpose")[0].physical_cells == (
        "register[0].purpose",
    )
    assert first.parent_facts[0].kind == "variant"
    assert first.parent_field_locators(0, "coverage_from")[0].physical_cells == (
        "register[0].variant[0].valid_from",
    )
    assert variable.fields.coverage_from is None and variable.fields.coverage_to is None
    assert variable.edition_period_scope.kind == "not_applicable"
    assert variable.subject.variant.status == "not_applicable"
    assert [
        reference.native_id for reference in variable.subject.variant_references
    ] == ["first", "second"]
    assert (
        variable.fields.identifier is not None
        and variable.fields.identifier.value is False
    )
    assert (
        variable.fields.sensitivity is not None
        and variable.fields.sensitivity.value is True
    )
    assert variable.fields.classification_declared is not None
    assert (
        variable.fields.classification_declared.value
        == "Unresolved declared classification"
    )
    assert variable.code_set_references == ()
    assert variable.subject.variable.native_id == "CODE"
    assert variable.subject.native.variable_id is None
    assert not clean.descriptors and not clean.associations


def test_omitted_empty_and_false_declarations_are_not_defaulted(tmp_path: Path) -> None:
    path, revision = _source(
        tmp_path,
        _BASE
        + _VARIABLE
        + """
definition = ""
variants = []
is_sensitive = false
classification = "   "
""",
    )
    clean = read_curated_source(path, revision, provider="agency")
    assert len(clean.records) == 2  # No synthesized variant or state.
    record = clean.records[-1]
    assert record.fields.identifier is None
    assert (
        record.fields.sensitivity is not None
        and record.fields.sensitivity.value is False
    )
    assert (
        record.fields.definition is not None
        and record.fields.definition.status == "unknown"
    )
    assert record.fields.definition.raw_value == ""
    assert (
        record.fields.classification_declared is not None
        and record.fields.classification_declared.raw_value == "   "
    )
    assert record.subject.variant_references == ()
    cells = {cell.name: cell for cell in record.delivered_cells}
    assert "is_identifier" not in cells
    assert (
        cells["is_sensitive"].raw_value == "false"
        and cells["is_sensitive"].raw_type == "bool"
    )
    assert cells["variants"].raw_value == "[]" and cells["variants"].raw_type == "list"
    assert cells["definition"].raw_value == ""


@pytest.mark.parametrize(
    "bounds,kind",
    [
        ('valid_from = "2010-01-01"', "unknown"),
        ('valid_to = "2010-12-31"', "unknown"),
        ('valid_from = "2010-01-01"\nvalid_to = "2012-12-31"', "intervals"),
        ('valid_from = "2012-01-01"\nvalid_to = "2010-12-31"', "unknown"),
        ('valid_from = "2010-02-30"\nvalid_to = "2010-12-31"', "unknown"),
    ],
)
def test_only_literal_complete_valid_bounds_form_a_date_scope(
    tmp_path: Path, bounds: str, kind: str
) -> None:
    path, revision = _source(tmp_path, _BASE + _VARIABLE + bounds)
    record = read_curated_source(path, revision, provider="agency").records[-1]
    assert record.edition_scope.kind == "not_applicable"
    assert record.edition_period_scope.kind == kind
    cells = {cell.name: cell.raw_value for cell in record.delivered_cells}
    for name, field in (
        ("valid_from", record.fields.coverage_from),
        ("valid_to", record.fields.coverage_to),
    ):
        if name in cells:
            assert field is not None and field.raw_value == cells[name]
        else:
            assert field is None


def test_repeated_conflicting_declarations_and_unmatched_refs_are_preserved(
    tmp_path: Path,
) -> None:
    path, revision = _source(
        tmp_path,
        _BASE
        + _VARIABLE
        + """
variants = ["not-yet-declared"]
is_sensitive = false
"""
        + _VARIABLE.replace('data_type = "text"', 'data_type = "integer"')
        + """
variants = ["not-yet-declared"]
is_sensitive = true
""",
    )
    clean = read_curated_source(path, revision, provider="agency")
    first, second = clean.records[-2:]
    assert (
        first.locators[0].semantic_record_key == second.locators[0].semantic_record_key
    )
    assert first.record_id != second.record_id
    assert first.locators[0].physical_record == "register[0].variable[0]"
    assert second.locators[0].physical_record == "register[0].variable[1]"
    assert first.fields.data_type is not None and first.fields.data_type.value == "text"
    assert (
        second.fields.data_type is not None
        and second.fields.data_type.value == "integer"
    )
    assert first.subject.variant_references[0].native_id == "not-yet-declared"
    rows = next(
        table.rows for table in clean.tables if table.name == "register.variable"
    )
    assert len(rows) == 2 and rows[0].cells == first.delivered_cells


@pytest.mark.parametrize(
    "addition",
    [
        'is_sensitive = "false"',
        "is_identifer = true",
        "variants = [1]",
        'state = [{column = "X"}]',
    ],
)
def test_broken_actual_format_contracts_fail_without_silent_fallback(
    tmp_path: Path, addition: str
) -> None:
    path, revision = _source(tmp_path, _BASE + _VARIABLE + addition)
    with pytest.raises(CuratedSourceError, match="invalid global curated TOML"):
        read_curated_source(path, revision, provider="agency")


def test_raw_toml_values_and_exact_key_locations_are_retained(tmp_path: Path) -> None:
    path, revision = _source(
        tmp_path,
        _BASE
        + _VARIABLE
        + """
definition = "  First\\n  second  "
variants = [" x ", "y", "y"]
""",
    )
    record = read_curated_source(path, revision, provider="agency").records[-1]
    cells = {cell.name: cell for cell in record.delivered_cells}
    assert cells["definition"].raw_value == "  First\n  second  "
    assert (
        record.fields.definition is not None
        and record.fields.definition.value == "  First\n  second"
    )
    assert json.loads(cells["variants"].raw_value or "null") == [" x ", "y", "y"]
    assert [reference.native_id for reference in record.subject.variant_references] == [
        "x",
        "y",
        "y",
    ]
    assert "register[0].variable[0].definition" in record.locators[0].physical_cells


def test_canonical_scb_uses_the_same_toml_reader_without_binding_codes(
    tmp_path: Path,
) -> None:
    path, revision = _source(
        tmp_path,
        _BASE + _VARIABLE + 'value_set = "riktning"',
        filename="scb_canonical.toml",
    )
    clean = read_curated_source(path, revision, provider="scb")
    record = clean.records[-1]
    assert record.subject.provider == "scb"
    assert (
        record.fields.value_set_declared is not None
        and record.fields.value_set_declared.value == "riktning"
    )
    assert clean.declared_value_lists == ("riktning",)
    assert record.code_set_references == ()
    assert clean.associations == ()


def test_reader_requires_exact_source_revision_bytes(tmp_path: Path) -> None:
    path, revision = _source(tmp_path, _BASE + _VARIABLE)
    path.write_text(path.read_text() + "\n# changed\n")
    with pytest.raises(CuratedSourceError, match="differs from its declared revision"):
        read_curated_source(path, revision, provider="agency")
