"""Resolved writer output for registers, variants, variables, states, editions and aliases."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import (
    resolved_state as _state,
    resolved_variable as _variable,
)
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedEdition,
    ResolvedObjectType,
    ResolvedPopulation,
    ResolvedRegister,
    ResolvedVariable,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


def test_delivery_names_and_descriptions_survive_without_common_text(tmp_path: Path):
    variable = _variable()
    states = tuple(
        state.model_copy(update={"name": name, "description": description})
        for state, name, description in zip(
            variable.states,
            ("Admission region", "Encounter region"),
            ("Region reporting an admission", "Region reporting an encounter"),
            strict=True,
        )
    )
    variable = ResolvedVariable.model_validate(
        variable.model_copy(
            update={"name": None, "description": None, "states": states}
        ).model_dump()
    )
    output = tmp_path / "catalog.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    assert validate_built_db(output, corpus=False).passed
    with closing(open_built_db(output)) as conn:
        row = conn.execute("SELECT name, description FROM variable").fetchone()
        assert tuple(row) == (None, None)
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT name, description FROM variable_state ORDER BY valid_from"
            )
        ] == [(state.name, state.description) for state in states]
        for text in ("Admission", "encounter"):
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM variable_fts WHERE variable_fts MATCH ?",
                    (text,),
                ).fetchone()[0]
                == 1
            )


def test_independent_parent_metadata_survives_without_variables_or_editions(
    tmp_path: Path,
) -> None:
    register = ResolvedRegister(provider="sos", slug="independent", name="Independent")
    variant = ResolvedVariant(slug="observations", name="Observations")
    other = ResolvedRegister(provider="sos", slug="other", name="Other")
    output = tmp_path / "diagnostic.db"
    write_resolved_catalog(
        (_variable(),),
        output,
        manifest=synthetic_manifest(),
        diagnostic=True,
        parent_registers=(other,),
        parent_variants=((register, variant),),
    )
    with closing(open_built_db(output)) as conn:
        assert {row[0] for row in conn.execute("SELECT slug FROM register")} == {
            "example",
            "independent",
            "other",
        }
        assert (
            conn.execute(
                "SELECT name FROM register_variant WHERE slug='observations'"
            ).fetchone()[0]
            == "Observations"
        )
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 1
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize("provider", ["scb", "sos"])
@pytest.mark.parametrize("variant", ["individuals", "_default"])
def test_catalog_graph_fts_and_structural_validation(
    tmp_path: Path, provider: str, variant: str
) -> None:
    variable = _variable(provider)
    variable = variable.model_copy(
        update={
            "states": tuple(
                state.model_copy(
                    update={
                        "variant": state.variant.model_copy(update={"slug": variant})
                    }
                )
                for state in variable.states
            )
        }
    )
    output = tmp_path / "reg_meta.db"
    assert (
        write_resolved_catalog(
            (variable,), output, manifest=synthetic_manifest() | {"input": "fixture"}
        )
        == output
    )
    validation = validate_built_db(output, corpus=False)
    assert validation.passed, validation.format_report()

    with closing(open_built_db(output)) as conn:
        assert conn.execute("SELECT name FROM register").fetchone()[0] == "Example"
        row = conn.execute("SELECT * FROM variable").fetchone()
        assert row["name"] == variable.name
        assert row["definition"] == variable.definition
        assert row["is_sensitive"] == 1
        assert row["is_identifier"] == 0
        assert row["provider_key"] == "44"
        assert conn.execute("SELECT COUNT(*) FROM variable_state").fetchone()[0] == 2
        for year in (1999, 2001, 2003):
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM variable_state WHERE valid_from <= ? AND valid_to >= ?",
                    (f"{year}-12-31", f"{year}-01-01"),
                ).fetchone()[0]
                == 0
            )
        state = conn.execute(
            "SELECT s.* FROM variable_state s JOIN register_variant v USING (register_variant_id) WHERE valid_from='2002-01-01' AND v.slug=?",
            (variant,),
        ).fetchone()
        assert (state["valid_from"], state["valid_to"]) == ("2002-01-01", "2002-12-31")
        assert state["delivery_column_name"] == "AmPolTypUpdated"
        assert (state["data_type"], state["data_length"]) == ("integer", "8")
        assert state["operational_definition"] == "State definition for 2002"
        assert state["provenance"] == "curation:fixture:2002"
        assert state["value_set_id"] is None
        for query in (
            "description:searchable",
            "delivery_column_names:AmPolTypUpdated",
        ):
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM variable_fts WHERE variable_fts MATCH ?",
                    (query,),
                ).fetchone()[0]
                == 1
            )
        assert conn.execute("SELECT count(*) FROM variable_alias").fetchone()[0] == 2
        assert (
            conn.execute("SELECT count(*) FROM variable_alias_window").fetchone()[0]
            == 0
        )
        assert conn.execute("SELECT count(*) FROM value_set").fetchone()[0] == 0


def test_resolved_metadata_is_written_without_source_inference(tmp_path: Path) -> None:
    variable = _variable()
    register = variable.register_ref.model_copy(update={"purpose": "Declared purpose"})
    variant = variable.states[0].variant.model_copy(
        update={
            "description": "Declared delivery population",
            "display_group": "Population tables",
            "panel_entity_key": (variable.slug,),
            "panel_time_key": "period",
            "panel_time_grain": "delivery",
        }
    )
    source = ResolvedRegister(provider="sos", slug="source", name="Source registry")
    variable = variable.model_copy(
        update={
            "register_ref": register,
            "source_register": source,
            "source_register_text": "Source as supplied",
            "source_label": "Resolved source label",
            "states": tuple(
                state.model_copy(
                    update={
                        "variant": variant,
                        "source_register_text": "State source",
                        "provenance": None,
                    }
                )
                for state in variable.states
            ),
        }
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert (
            conn.execute(
                "SELECT purpose FROM register WHERE slug='example'"
            ).fetchone()[0]
            == "Declared purpose"
        )
        row = conn.execute(
            "SELECT description, display_group, panel_entity_key, panel_time_key, panel_time_grain FROM register_variant"
        ).fetchone()
        assert tuple(row) == (
            "Declared delivery population",
            "Population tables",
            '["ampoltyp"]',
            "period",
            "delivery",
        )
        row = conn.execute(
            "SELECT r.slug, v.source_register_text, v.source_label FROM variable v JOIN register r ON r.register_id=v.source_register_id"
        ).fetchone()
        assert tuple(row) == ("source", "Source as supplied", "Resolved source label")
        assert tuple(
            conn.execute(
                "SELECT DISTINCT source_register_text, provenance FROM variable_state"
            ).fetchone()
        ) == ("State source", None)


def test_edition_prose_population_and_object_types_do_not_change_state_periods(
    tmp_path: Path,
) -> None:
    variable = _variable()
    edition = ResolvedEdition(
        register=variable.register_ref,
        variant=variable.states[0].variant,
        name="2000-2005 pooled edition",
        description="Edition description",
        measurement_information="Measured at source reference time",
        documentation_status="Preliminary",
        first_approved_at="2006-02-03 12:34:56",
        last_approved_at="Not dated",
        populations=(
            ResolvedPopulation(
                name="People",
                definition="Population definition",
                comment="Scope note",
                date_range="2000–2005",
            ),
            ResolvedPopulation(name="Another population"),
        ),
        object_types=(
            ResolvedObjectType(name="Person", definition="Object definition"),
        ),
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (variable,), output, manifest=synthetic_manifest(), editions=(edition,)
    )
    original = output.read_bytes()
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT registerversionnamn, registerversionbeskrivning, registerversionmatinformation, "
                "registerversion_docstaus, registerversion_forstagodkannandedatum, registerversion_senastgodkanddatum "
                "FROM register_version"
            ).fetchone()
        ) == (
            edition.name,
            edition.description,
            edition.measurement_information,
            edition.documentation_status,
            edition.first_approved_at,
            edition.last_approved_at,
        )
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT name, definition, comment, date_range FROM population ORDER BY name"
            )
        ] == [
            ("Another population", None, None, None),
            ("People", "Population definition", "Scope note", "2000–2005"),
        ]
        assert tuple(
            conn.execute("SELECT name, definition FROM object_type").fetchone()
        ) == (
            "Person",
            "Object definition",
        )
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT valid_from, valid_to FROM variable_state ORDER BY valid_from"
            )
        ] == [("2000-01-01", "2000-12-31"), ("2002-01-01", "2002-12-31")]
    write_resolved_catalog(
        (variable,),
        output,
        manifest=synthetic_manifest(),
        editions=(
            edition.model_copy(
                update={"populations": tuple(reversed(edition.populations))}
            ),
        ),
    )
    assert output.read_bytes() == original


def test_alias_windows_and_historical_search_aliases_are_explicit(
    tmp_path: Path,
) -> None:
    variable = _variable()
    variant = variable.states[0].variant
    window = ResolvedAliasWindow(valid_from="2000-01-01", valid_to="2000-12-31")
    aliases = (
        ResolvedAlias(
            variant=variant, delivery_column_name="AmPolTyp", windows=(window,)
        ),
        ResolvedAlias(
            variant=variant, delivery_column_name="ParallelColumn", windows=(window,)
        ),
        ResolvedAlias(variant=variant, delivery_column_name="HistoricalSearchOnly"),
    )
    variable = variable.model_copy(update={"aliases": aliases})
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    original = output.read_bytes()
    with closing(open_built_db(output)) as conn:
        rows = conn.execute(
            "SELECT delivery_column_name, valid_from, valid_to, provenance FROM variable_alias_window ORDER BY delivery_column_name"
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("AmPolTyp", "2000-01-01", "2000-12-31", None),
            ("ParallelColumn", "2000-01-01", "2000-12-31", None),
        ]
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM variable_alias WHERE delivery_column_name='HistoricalSearchOnly'"
            ).fetchone()[0]
            == 1
        )
    write_resolved_catalog(
        (variable.model_copy(update={"aliases": tuple(reversed(aliases))}),),
        output,
        manifest=synthetic_manifest(),
    )
    assert output.read_bytes() == original


@pytest.mark.parametrize(("sensitive", "identifier"), [(True, False), (False, True)])
def test_flags_preserve_explicit_booleans(
    tmp_path: Path, sensitive: bool, identifier: bool
) -> None:
    variable = _variable().model_copy(
        update={"is_sensitive": sensitive, "is_identifier": identifier}
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        row = conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable"
        ).fetchone()
        assert tuple(row) == (sensitive, identifier)


def test_year_independent_writer_preserves_null_bounds_and_guards_scope_mixing(
    tmp_path,
):
    independent = _state(2000).model_copy(
        update={
            "period_scope": "year_independent",
            "valid_from": None,
            "valid_to": None,
        }
    )
    variable = _variable().model_copy(update={"states": (independent,)})
    output = tmp_path / "independent.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as connection:
        assert tuple(
            connection.execute(
                "SELECT period_scope, valid_from, valid_to, pooled FROM variable_state"
            ).fetchone()
        ) == ("year_independent", None, None, 0)
    for states in ((independent, independent), (independent, _state(2000))):
        bad = variable.model_copy(update={"states": states})
        with pytest.raises(
            ValidationError, match="(duplicate year-independent|mixed dated)"
        ):
            write_resolved_catalog(
                (bad,), tmp_path / "bad.db", manifest=synthetic_manifest()
            )
    with pytest.raises(ValidationError, match="dated alias windows"):
        write_resolved_catalog(
            (
                variable.model_copy(
                    update={
                        "aliases": (
                            ResolvedAlias(
                                variant=independent.variant,
                                delivery_column_name="Old",
                                windows=(
                                    ResolvedAliasWindow(
                                        valid_from="2000-01-01", valid_to="2000-12-31"
                                    ),
                                ),
                            ),
                        )
                    }
                ),
            ),
            tmp_path / "bad-alias.db",
            manifest=synthetic_manifest(),
        )
