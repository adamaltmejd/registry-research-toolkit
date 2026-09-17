"""Literal export metadata stays separate from catalog identity/relationships."""

from __future__ import annotations

from typing import cast

import pytest
from pydantic import ValidationError
from reg_meta_build.source_records import (
    DeliveredCell,
    RecordLocator,
    SourceField,
    SourceRevision,
    value_field,
)
from reg_meta_build.source_reference_records import (
    SourceCodeCrosswalkDeclaration,
    SourceColumnTypeDeclaration,
    SourceEventDeclaration,
    SourceJoinKeyDeclaration,
)
from reg_meta_build.source_reference_resolution import (
    ExportDeclaration,
    resolve_export_metadata,
)

REVISION = SourceRevision.create(
    dataset="schema",
    publisher="fixture",
    purpose="test",
    upstream_revision="1",
    artifact_path="schema.sql",
    artifact_size=1,
    artifact_sha256="a" * 64,
)
LOCATOR = RecordLocator(
    semantic_record_key=("T", "ID"),
    physical_file="schema.sql",
    physical_table="T",
    physical_record="line:2",
    physical_cells=("line:2",),
)


def column(*, nullable: bool = True) -> SourceColumnTypeDeclaration:
    return SourceColumnTypeDeclaration(
        revision=REVISION,
        locator=LOCATOR,
        delivered_cells=(),
        table_name=value_field("T"),
        column_name=value_field("ID"),
        data_type=value_field("varchar"),
        declared_width=value_field(12),
        nullable=value_field(nullable),
    )


def join(description: str | None = "Identifier") -> SourceJoinKeyDeclaration:
    return SourceJoinKeyDeclaration(
        revision=REVISION,
        locator=LOCATOR.model_copy(update={"semantic_record_key": ("key", "T", "ID")}),
        delivered_cells=(),
        table_name=value_field("T"),
        column_name=value_field("ID"),
        description=value_field(description)
        if description is not None
        else SourceField(status="unknown"),
    )


def test_export_metadata_coalesces_equal_assertions_and_keeps_known_optional_text() -> (
    None
):
    declarations = (column(), column(), join(), join(None))
    result = resolve_export_metadata(declarations)
    assert not result.diagnostics and not result.withheld
    assert (
        len(result.metadata.source_columns)
        == len(result.metadata.source_join_keys)
        == 1
    )
    assert result.metadata.source_columns[0].sql_type == "varchar(12)"
    assert result.metadata.source_columns[0].nullable
    assert result.metadata.source_join_keys[0].description == "Identifier"
    assert resolve_export_metadata(reversed(declarations)) == result


def test_conflicting_export_types_withhold_the_column_and_its_dependent_key() -> None:
    result = resolve_export_metadata((column(), column(nullable=False), join()))
    assert not result.metadata.source_columns and not result.metadata.source_join_keys
    assert {d.code for d in result.diagnostics} == {
        "conflicting_export_column_type",
        "unsupported_export_join_key",
    }
    assert {ref.semantic_record_key for ref in result.withheld} == {
        ("T", "ID"),
        ("key", "T", "ID"),
    }
    assert all(d.severity == "error" and d.refs for d in result.diagnostics)


def test_disputed_join_description_does_not_erase_independent_column_metadata() -> None:
    result = resolve_export_metadata((column(), join(), join("Different")))
    assert (
        len(result.metadata.source_columns) == 1
        and not result.metadata.source_join_keys
    )
    assert result.diagnostics[0].code == "conflicting_export_join_key"


def test_unknown_required_type_metadata_is_not_filled_with_defaults() -> None:
    unknown = column().model_copy(update={"nullable": SourceField(status="unknown")})
    result = resolve_export_metadata((unknown, join()))
    assert not result.metadata.source_columns and not result.metadata.source_join_keys
    assert result.diagnostics[0].code == "unknown_export_column_type"
    invalid = column().model_copy(update={"nullable": value_field("yes")})
    with pytest.raises(ValidationError):
        resolve_export_metadata((invalid,))


def test_event_metadata_retains_literal_tokens_without_creating_relationships() -> None:
    cell = DeliveredCell(
        name="ID1", present=True, raw_value="0012", interpreted_value="0012"
    )
    missing = DeliveredCell(
        name="ID2", present=False, raw_value=None, interpreted_value=""
    )
    event = SourceEventDeclaration(
        revision=REVISION,
        locator=LOCATOR,
        delivered_cells=(cell, missing),
        name=value_field("Event"),
        action="replaced_by",
        action_label=value_field("Replaced by"),
        description=SourceField(status="unknown"),
        entity_kind="variable",
        entity_label=value_field("Variable"),
        first_token=cell,
        second_token=missing,
        document_token=missing,
    )
    result = resolve_export_metadata((event, event))
    assert not result.diagnostics and len(result.metadata.timeseries_events) == 2
    assert result.metadata.timeseries_events[0].first_token == "0012"
    assert result.metadata.timeseries_events[0].second_token is None
    assert result.metadata.successions == ()


def test_other_source_declarations_are_not_silently_discarded() -> None:
    crosswalk = SourceCodeCrosswalkDeclaration(
        revision=REVISION,
        locator=LOCATOR,
        delivered_cells=(),
        member_name=None,
        supplied_period=None,
        section_period=None,
        section_locator=None,
        description=None,
        operands=(),
    )
    with pytest.raises(TypeError, match="schema or event"):
        resolve_export_metadata((cast("ExportDeclaration", crosswalk),))
