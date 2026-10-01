"""Actual authored TOML/CSV layouts stay source evidence before resolution."""

from __future__ import annotations

import hashlib
import json
import tomllib
from pathlib import Path

import pytest
from reg_meta.fqid import derive_variable_slug
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.sources.curated_records import (
    CuratedSourceError,
    read_curated_source,
)


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


def test_omitted_flag_keys_read_as_explicit_false(tmp_path: Path) -> None:
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
    assert (
        record.fields.identifier is not None and record.fields.identifier.value is False
    )
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
    # Omission still leaves no evidence cell: the false claim has no raw bytes.
    assert "is_identifier" not in cells
    assert (
        cells["is_sensitive"].raw_value == "false"
        and cells["is_sensitive"].raw_type == "bool"
    )
    assert cells["variants"].raw_value == "[]" and cells["variants"].raw_type == "list"
    assert cells["definition"].raw_value == ""
    bare_path, bare_revision = _source(tmp_path, _BASE + _VARIABLE)
    bare = read_curated_source(bare_path, bare_revision, provider="agency").records[-1]
    assert bare.fields.identifier is not None and bare.fields.identifier.value is False
    assert (
        bare.fields.sensitivity is not None and bare.fields.sensitivity.value is False
    )


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
        'is_sensitive = ""',
        'is_identifier = ""',
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


def test_fohm_thin_slug_entries_follow_authored_columns() -> None:
    root = Path(__file__).parents[1]
    source = tomllib.loads(
        (root / "input_data/Folkhalsomyndigheten/fohm.toml").read_text()
    )
    for register, columns in (
        ("sminet", {"provtagningsdatum", "statistikdatum"}),
        ("nvr", {"nplid"}),
    ):
        declared = next(item for item in source["register"] if item["key"] == register)
        assert columns <= {item["column"] for item in declared["variable"]}
        auto = tomllib.loads(
            (root / f"curation/registers/fohm/{register}.auto.toml").read_text()
        )
        for column in columns:
            entry = next(
                item
                for item in auto["variable"]
                if item["native_id"].endswith(f".{column}")
            )
            assert entry["slug"] == derive_variable_slug(column)


def test_pooled_delivery_remains_pooled_through_checked_thin_compilation(tmp_path):
    from reg_meta_build.curation_compile import _compile_thin_register
    from reg_meta_build.source_curation import CuratedOccurrenceAddition
    from reg_meta_build.source_effects import apply_occurrence_cases

    path, revision = _source(
        tmp_path,
        """
[[register]]
key = "r"
name = "Register with unknown inception"
[[register.variant]]
key = "table"
name = "Pooled table"
valid_from = "2020-01-01"
valid_to = "2021-12-31"
period_scope = "pooled"
[[register.variable]]
name = "Literal header"
column = "HEADER"
data_type = "text"
data_warning = "Header-only metadata; response codes unavailable."
variants = ["table"]
valid_from = "2020-06-01"
valid_to = "2021-06-30"
""",
    )
    clean = read_curated_source(path, revision, provider="agency")
    register, variant, variable = clean.records
    assert register.parent_facts[0].fields.coverage_from is None
    assert variant.edition_period_scope.kind == "pooled"
    assert any(
        cell.name == "period_scope" and cell.raw_value == "pooled"
        for cell in variant.delivered_cells
    )
    cases = _compile_thin_register(clean.records, ())
    assert (
        cases[0].decision.data_warning
        == "Header-only metadata; response codes unavailable."
    )
    assert len(cases[0].decision.data_warning_refs) == 1
    assert cases[0].decision.data_warning_fields == ("name", "definition", "coding")
    additions = [
        effect
        for case in cases
        for effect in case.decision.effects
        if isinstance(effect, CuratedOccurrenceAddition)
    ]
    assert len(additions) == 1
    scope = additions[0].edition_period_scope
    assert (scope.kind, scope.pooled_start, scope.pooled_end) == (
        "pooled",
        "2020-06-01",
        "2021-06-30",
    )
    assert scope.intervals == ()
    applied = apply_occurrence_cases(clean.records, cases)
    assert not applied.diagnostics
    physical = [
        occurrence
        for occurrence in applied.occurrences
        if occurrence.use == "catalog" and occurrence.variable_key is not None
    ]
    assert len(physical) == 1
    assert physical[0].edition_period_scope == scope
    assert physical[0].support_records == (variable,)


@pytest.mark.parametrize(
    "bounds",
    [
        "",
        'valid_from = "2020-01-01"',
        'valid_to = "2021-12-31"',
        'valid_from = "2021-12-31"\nvalid_to = "2020-01-01"',
        'valid_from = "2020"\nvalid_to = "2021"',
        'valid_from = "2020-02-30"\nvalid_to = "2021-12-31"',
    ],
)
def test_pooled_delivery_requires_explicit_valid_ordered_iso_bounds(tmp_path, bounds):
    path, revision = _source(
        tmp_path,
        _BASE
        + '\n[[register.variant]]\nkey = "table"\nname = "Table"\nperiod_scope = "pooled"\n'
        + bounds,
    )
    with pytest.raises(CuratedSourceError, match="pooled|day.*range"):
        read_curated_source(path, revision, provider="agency")


@pytest.mark.parametrize("warning", ['""', '"   "', "false", "12"])
def test_thin_data_warning_requires_nonblank_text(tmp_path, warning):
    path, revision = _source(
        tmp_path, _BASE + _VARIABLE + f"\ndata_warning = {warning}\n"
    )
    with pytest.raises(CuratedSourceError, match="data_warning"):
        read_curated_source(path, revision, provider="agency")
