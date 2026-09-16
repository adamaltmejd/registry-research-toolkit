"""Common parent resolution supplies topology to ordinary variable formation."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta.db import open_db
from reg_meta_build.catalog_resolution import resolve_parents
from reg_meta_build.resolved_catalog import write_resolved_catalog
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_parent_key,
    source_register_key,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import SourceFields, SourceRevision, value_field
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_records import SourceRecord


_REVISION = SourceRevision.create(
    dataset="fixture",
    publisher="SCB",
    purpose="common parent integration",
    upstream_revision="1",
    artifact_path="Registerinformation.csv",
    artifact_size=1,
    artifact_sha256="a" * 64,
)


def _record(row: int = 2, **changes: str) -> SourceRecord:
    header = REGISTERINFORMATION_HEADER.split("|")
    raw = (
        dict(
            zip(
                header,
                _var_row(colname="VALUE", cvid=4, var_id=5).split("|"),
                strict=True,
            )
        )
        | changes
    )
    cells: dict[str, tuple[bool, str | None, str]] = {
        name: (True, value, value) for name, value in raw.items()
    }
    return clean_scb_row(header, row, cells, _REVISION).record


def _names(record: SourceRecord) -> tuple[NamingDeclaration, ...]:
    # Exact checked identities are inputs of parent resolution; applicability itself
    # is exercised by the naming and pipeline-boundary tests.
    result = []
    for parent in record.parent_facts:
        if parent.kind not in {"register", "variant"}:
            continue
        key = native_parent_key(record.source, record.subject.provider, parent)
        assert key is not None
        kind = "register" if parent.kind == "register" else "register_variant"
        result.append(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind=kind,
                    provider="scb",
                    source_key=key,
                    register_key=source_register_key(record)
                    if parent.kind == "variant"
                    else None,
                    identity_revision=_REVISION,
                ),
                naming=SlugEntry(
                    kind=kind,
                    provider="scb",
                    source_id="1" if parent.kind == "register" else "1.10",
                    slug="example" if parent.kind == "register" else "people",
                ),
                contributors=(),
            )
        )
    return tuple(result)


def test_parent_facts_feed_ordinary_formation_and_direct_catalog(
    tmp_path: Path,
) -> None:
    record = _record()
    parents = resolve_parents((record, record), _names(record))
    assert parents.diagnostics == ()
    assert len(parents.registers) == len(parents.variants) == len(parents.editions) == 1
    register_key, code_key = source_register_key(record), native_column_key(record)
    assert register_key is not None and code_key is not None
    formed = form_native_variable(
        (record,),
        register=parents.registers[register_key],
        variants=parents.variants,
        slug="value",
        provider_key="5",
        coding={code_key: ()},
        flags=SourceFields(
            identifier=value_field(False), sensitivity=value_field(False)
        ),
    )
    assert formed.variable is not None and formed.diagnostics == ()
    output = tmp_path / "catalog.db"
    write_resolved_catalog(
        (formed.variable,),
        output,
        manifest={"fixture": "parent-resolution"},
        editions=tuple(parents.editions.values()),
    )
    with closing(open_db(output)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM register_version").fetchone()[0] == 1
        assert (
            conn.execute("SELECT delivery_column_name FROM variable_state").fetchone()[
                0
            ]
            == "VALUE"
        )
        assert (
            conn.execute("SELECT name FROM population").fetchone()[0]
            == "Hela befolkningen"
        )


def test_parent_conflict_does_not_choose_first_prose_or_change_delivery_dates() -> None:
    first, other = _record(), _record(3, Registersyfte="Conflicting purpose")
    parents = resolve_parents((first, other), _names(first))
    assert len(parents.registers) == 1
    register = next(iter(parents.registers.values()))
    assert register.purpose is None
    assert len(parents.diagnostics) == 1
    assert parents.diagnostics[0].fields == ("purpose",)
    assert parents.diagnostics[0].withheld_output == ("register.purpose",)
    assert len(parents.editions) == 1
    assert parents == resolve_parents((other, first), _names(first))


def test_edition_populations_do_not_become_competing_variable_assignments() -> None:
    records = (
        _record(Populationnamn="Adults"),
        _record(3, Populationnamn="Children"),
    )
    parents = resolve_parents(records, _names(records[0]))
    register_key = source_register_key(records[0])
    column_key = native_column_key(records[0])
    assert register_key is not None and column_key is not None
    formed = form_native_variable(
        records,
        register=parents.registers[register_key],
        variants=parents.variants,
        slug="value",
        provider_key="5",
        flags=SourceFields(
            identifier=value_field(False), sensitivity=value_field(False)
        ),
        coding={column_key: ()},
    )
    assert formed.variable is not None and formed.diagnostics == ()
    assert len(formed.variable.states) == 1
    assert formed.occurrences == records
    assert {
        population.name
        for edition in parents.editions.values()
        for population in edition.populations
    } == {"Adults", "Children"}


def test_missing_naming_is_an_implementation_failure() -> None:
    with pytest.raises(ValueError, match="missing checked parent naming"):
        resolve_parents((_record(),), ())


def test_parent_translations_are_accounted_without_conflicting_with_requested_language() -> (
    None
):
    swedish = _record().model_copy(update={"language": "sv"})
    english = _record(3, Registersyfte="English purpose").model_copy(
        update={"language": "en"}
    )
    parents = resolve_parents((swedish, english), _names(swedish))
    assert parents.diagnostics == ()
    assert parents.registers == resolve_parents((swedish,), _names(swedish)).registers
    assert parents.other_language_refs == (record_ref(english),)
    translated = resolve_parents((swedish, english), _names(swedish), language="en")
    assert next(iter(translated.registers.values())).purpose == "English purpose"
    assert translated.other_language_refs == (record_ref(swedish),)
