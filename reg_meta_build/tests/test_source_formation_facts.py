"""Native family formation: canonical text, unit, flag and sensitivity facts at variable and state grain."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta.source_evidence import SourceField
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    write_resolved_catalog,
)
from reg_meta_build.source_occurrences import (
    source_occurrence,
)
from reg_meta_build.source_records import (
    SourceFields,
    SourceRecord,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path
from _source_formation_support import form_family as _form, formation_record as _record


def test_canonical_text_conflict_preserves_states_and_withholds_only_that_fact() -> (
    None
):
    result = _form((_record(2020), _record(2021, definition="Conflicting definition")))
    assert result.variable is not None
    assert result.variable.definition is None
    assert len(result.variable.states) == 2
    assert [(d.code, d.fields, d.withheld_output) for d in result.diagnostics] == [
        ("conflicting_variable_fact", ("definition",), ("variable.definition",))
    ]


@pytest.mark.parametrize("field", ["name", "definition", "description"])
def test_variable_grain_canonical_conflict_fields_are_unchanged(field: str) -> None:
    first = _record(2020)
    second = _record(2021)
    first = first.model_copy(
        update={
            "fields": first.fields.model_copy(update={field: value_field("First text")})
        }
    )
    second = second.model_copy(
        update={
            "fields": second.fields.model_copy(
                update={field: value_field("Conflicting text")}
            )
        }
    )
    result = _form((first, second))
    assert [
        (d.fields, d.withheld_output)
        for d in result.diagnostics
        if d.code == "conflicting_variable_fact"
    ] == [((field,), (f"variable.{field}",))]


@pytest.mark.parametrize(
    ("published", "other"),
    [
        ("SEK", "kr"),
        ("Kronor", "kronor"),
        ("Kronor (SEK)", "kronor"),
        ("Antal månader", "Månader"),
        ("Månader", "Månad"),
        ("Antal veckor", "Veckor"),
        ("Antal minuter", "Minuter"),
        ("Antal barn", "Antal"),
        ("Dagar", "Antal"),
        ("Årtal", "År"),
    ],
)
def test_exact_unit_pair_publishes_variable_unit_in_either_order(
    published: str, other: str
) -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"measurement_unit": value_field(unit)}
                )
            }
        )
        for record, unit in ((_record(2020), published), (_record(2021), other))
    )
    for ordered in (records, records[::-1]):
        result = _form(ordered)
        assert result.variable is not None
        assert result.variable.measurement_unit == published
        assert not any(
            diagnostic.code == "conflicting_variable_fact"
            for diagnostic in result.diagnostics
        )


def test_state_grain_texts_vary_by_period_without_a_variable_fact_conflict(
    tmp_path: Path,
) -> None:
    records = (
        _record(
            2020,
            operational_definition="Introductory question text",
            source_attribution="Fr171",
        ),
        _record(
            2021,
            operational_definition="Derived-variable definition",
            source_attribution="Fr128e",
        ),
    )
    result = _form(records)
    assert result.variable is not None
    assert result.diagnostics == ()
    assert result.variable.operational_definition is None
    assert result.variable.source_register_text is None
    output = tmp_path / "catalog.db"
    write_resolved_catalog((result.variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT operational_definition, source_register_text FROM variable"
            ).fetchone()
        ) == (None, None)
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT valid_from, operational_definition, source_register_text "
                "FROM variable_state ORDER BY valid_from"
            )
        ] == [
            ("2020-01-01", "Introductory question text", "Fr171"),
            ("2021-01-01", "Derived-variable definition", "Fr128e"),
        ]


def test_state_grain_texts_vary_by_variant_without_a_variable_fact_conflict() -> None:
    result = _form(
        (
            _record(
                2020,
                variant=2,
                operational_definition="Person wording",
                source_attribution="Fr171",
            ),
            _record(
                2020,
                variant=3,
                operational_definition="Household wording",
                source_attribution="Fr128e",
            ),
        )
    )
    assert result.variable is not None
    assert result.diagnostics == ()
    assert result.variable.operational_definition is None
    assert result.variable.source_register_text is None
    assert sorted(
        (state.variant.slug, state.operational_definition, state.source_register_text)
        for state in result.variable.states
    ) == [
        ("households", "Household wording", "Fr128e"),
        ("people", "Person wording", "Fr171"),
    ]


def test_stable_state_grain_texts_still_summarize_the_variable() -> None:
    result = _form(
        tuple(
            _record(
                year,
                operational_definition="One definition",
                source_attribution="Fr171",
            )
            for year in (2020, 2021)
        )
    )
    assert result.variable is not None
    assert result.diagnostics == ()
    assert result.variable.operational_definition == "One definition"
    assert result.variable.source_register_text == "Fr171"
    assert {
        (state.operational_definition, state.source_register_text)
        for state in result.variable.states
    } == {("One definition", "Fr171")}


@pytest.mark.parametrize("field", ["operational_definition", "source_attribution"])
def test_competing_same_period_texts_resolve_to_unknown_without_diagnostic(
    field: str,
) -> None:
    def competing(text: str, row: str) -> SourceRecord:
        return _record(
            2020,
            row=row,
            operational_definition=text if field == "operational_definition" else None,
            source_attribution=text if field == "source_attribution" else None,
        )

    first, second = competing("First text", ""), competing("Second text", "b")
    result = _form((first, second))
    assert result.variable is not None
    assert result.diagnostics == ()
    assert [
        (state.operational_definition, state.source_register_text)
        for state in result.variable.states
    ] == [(None, None)]


def test_checked_identity_does_not_choose_a_parallel_column_representation() -> None:
    records = (
        replace(source_occurrence(_record(2020, column="OLD")), identity_checked=True),
        replace(source_occurrence(_record(2020, column="NEW")), identity_checked=True),
        replace(source_occurrence(_record(2021, column="NEW")), identity_checked=True),
    )
    result = _form(records)
    assert result.variable is not None
    assert [
        (state.delivery_column_name, state.valid_from, state.valid_to)
        for state in result.variable.states
    ] == [("NEW", "2021-01-01", "2021-12-31")]
    assert [
        (issue.code, issue.valid_from, issue.valid_to, issue.withheld_output)
        for issue in result.diagnostics
    ] == [("unresolved_column_representation", "2020-01-01", "2020-12-31", ("state",))]
    assert result.occurrences == records


def test_unknown_flags_withhold_unsupported_entity_without_defaulting_false() -> None:
    result = _form((_record(2020),), flags=SourceFields())
    assert result.variable is None
    assert result.diagnostics[0].code == "unresolved_flag"
    assert result.diagnostics[0].fields == ("is_sensitive", "is_identifier")
    assert len(result.intervals[0].segments) == 1


@pytest.mark.parametrize(
    ("sensitivity", "conditional", "expected"),
    [
        (False, True, True),
        (True, True, True),
        (None, True, True),
        (False, False, False),
        (True, False, True),
    ],
)
def test_conditional_sensitivity_ratchets_up_without_changing_plain_values(
    sensitivity: bool | None, conditional: bool, expected: bool
) -> None:
    declaration = value_field(sensitivity) if sensitivity is not None else None
    result = _form(
        (_record(2020),),
        flags=SourceFields(
            sensitivity=declaration,
            identifier=value_field(False),
            conditional_sensitivity=value_field(conditional),
        ),
    )
    assert result.variable is not None
    assert result.variable.is_sensitive is expected
    assert result.diagnostics == ()


def test_conditional_false_alone_does_not_supply_sensitivity() -> None:
    result = _form(
        (_record(2020),),
        flags=SourceFields(
            identifier=value_field(False), conditional_sensitivity=value_field(False)
        ),
    )
    assert result.variable is None
    assert result.diagnostics[0].code == "unresolved_flag"
    assert result.diagnostics[0].fields == ("is_sensitive",)


def test_unknown_conditional_declaration_ratchets_to_sensitive() -> None:
    result = _form(
        (_record(2020),),
        flags=SourceFields(
            sensitivity=value_field(False),
            identifier=value_field(False),
            conditional_sensitivity=SourceField(status="unknown", raw_value="unclear"),
        ),
    )
    assert result.variable is not None
    assert result.variable.is_sensitive is True
