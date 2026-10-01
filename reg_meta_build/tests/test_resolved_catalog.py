"""The resolved writer publishes normal catalogs without source reconciliation."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from reg_meta.db import CLASSIFICATION_SUCCESSION_AS_OF_YEAR
from reg_meta.errors import RegMetaError
from reg_meta_build._curation import SentinelCode
from reg_meta_build.db import open_built_db, publish_db
from reg_meta_build.resolved_catalog import (
    CURATION_TREE_SHA256_KEY,
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedClassification,
    ResolvedClassificationCode,
    ResolvedClassificationLink,
    ResolvedClassificationSuccession,
    ResolvedCodeSet,
    ResolvedConformance,
    ResolvedEdition,
    ResolvedObjectType,
    ResolvedPopulation,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
    column_state_overlaps,
    validate_resolved_variables,
    write_resolved_catalog,
)
from reg_meta_build.validate import ValidationResult, validate_built_db

from reg_meta_build import resolved_catalog

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_scope import ScopeResolution


def _state(year: int, *, column: str = "AmPolTyp") -> ResolvedState:
    return ResolvedState(
        variant=ResolvedVariant(slug="individuals", name="Individuals"),
        valid_from=f"{year}-01-01",
        valid_to=f"{year}-12-31",
        delivery_column_name=column,
        data_type="integer",
        data_length="8",
        operational_definition=f"State definition for {year}",
        provenance=f"curation:fixture:{year}",
    )


def _variable(provider: str = "scb", slug: str = "ampoltyp") -> ResolvedVariable:
    return ResolvedVariable(
        register=ResolvedRegister(provider=provider, slug="example", name="Example"),
        slug=slug,
        provider_key="44",
        name="Arbetsmarknadspolitisk åtgärd",
        definition="An explicitly resolved definition",
        description="A searchable description",
        operational_definition="The canonical operational definition",
        measurement_unit=None,
        is_sensitive=True,
        is_identifier=False,
        states=(_state(2000), _state(2002, column="AmPolTypUpdated")),
    )


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
    write_resolved_catalog((variable,), output, manifest={})
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


def test_missing_common_name_still_requires_positive_delivery_names():
    with pytest.raises(ValidationError, match="positive delivery names"):
        ResolvedVariable.model_validate(
            _variable().model_copy(update={"name": None}).model_dump()
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
        manifest={},
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


@pytest.mark.parametrize("kind", ["register", "variant"])
def test_conflicting_independent_parents_fail_before_replacing_output(
    tmp_path: Path,
    kind: str,
) -> None:
    variable = _variable()
    output = tmp_path / "catalog.db"
    write_resolved_catalog((variable,), output, manifest={})
    before = output.read_bytes()
    with pytest.raises(ValueError, match=f"inconsistent resolved parent {kind}"):
        write_resolved_catalog(
            (variable,),
            output,
            manifest={},
            parent_registers=(
                variable.register_ref.model_copy(update={"name": "Conflict"}),
            )
            if kind == "register"
            else (),
            parent_variants=(
                (
                    variable.register_ref,
                    variable.states[0].variant.model_copy(update={"name": "Conflict"}),
                ),
            )
            if kind == "variant"
            else (),
        )
    assert output.read_bytes() == before


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
        write_resolved_catalog((variable,), output, manifest={"input": "fixture"})
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


def test_rerun_is_byte_identical_regardless_of_input_order(tmp_path: Path) -> None:
    variables = (_variable(), _variable("sos"))
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(variables, output, manifest={"z": "last", "a": "first"})
    original = output.read_bytes()
    reversed_variables = tuple(
        v.model_copy(update={"states": tuple(reversed(v.states))})
        for v in reversed(variables)
    )
    write_resolved_catalog(
        reversed_variables, output, manifest={"a": "first", "z": "last"}
    )
    assert output.read_bytes() == original
    assert output.with_name("reg_meta.db.prev").read_bytes() == original


@pytest.mark.parametrize("key", (CURATION_TREE_SHA256_KEY,))
@pytest.mark.parametrize("digest", ("short", "A" * 64))
def test_curation_manifest_hashes_require_lowercase_sha256(
    tmp_path: Path, key: str, digest: str
) -> None:
    output = tmp_path / "reg_meta.db"
    with pytest.raises(ValueError, match=f"manifest {key} must be a lowercase SHA-256"):
        write_resolved_catalog((_variable(),), output, manifest={key: digest})
    assert not output.exists()


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
    write_resolved_catalog((variable,), output, manifest={})
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
    write_resolved_catalog((variable,), output, manifest={}, editions=(edition,))
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
        manifest={},
        editions=(
            edition.model_copy(
                update={"populations": tuple(reversed(edition.populations))}
            ),
        ),
    )
    assert output.read_bytes() == original


@pytest.mark.parametrize("defect", ["register", "variant", "duplicate"])
def test_inconsistent_edition_metadata_preserves_previous_catalog(
    tmp_path: Path,
    defect: str,
) -> None:
    variable = _variable()
    edition = ResolvedEdition(
        register=variable.register_ref, variant=variable.states[0].variant, name="2000"
    )
    editions = (edition,)
    if defect == "register":
        editions = (
            edition.model_copy(
                update={
                    "register_ref": variable.register_ref.model_copy(
                        update={"name": "Conflicting name"}
                    )
                }
            ),
        )
    elif defect == "variant":
        editions = (
            edition.model_copy(
                update={
                    "variant": edition.variant.model_copy(
                        update={"name": "Conflicting name"}
                    )
                }
            ),
        )
    else:
        editions = (edition, edition)
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError, match="inconsistent|duplicate"):
        write_resolved_catalog((variable,), output, manifest={}, editions=editions)
    assert output.read_bytes() == b"previous"


def _classification(slug: str = "example-codes") -> ResolvedClassification:
    return ResolvedClassification(
        slug=slug,
        short_name=slug,
        name="Canonical example",
        name_en="Example",
        publisher="Publisher",
        valid_from=2000,
        valid_to=2020,
        description="Codebook description",
        url="https://example.invalid/codebook",
        codes=(
            ResolvedClassificationCode(code="001", label="Canonical one", level=1),
            ResolvedClassificationCode(code="002", label="Canonical two", level=2),
        ),
    )


def test_sentinel_overlapping_canonical_code_is_refused() -> None:
    with pytest.raises(ValidationError, match="example-codes") as exc_info:
        ResolvedClassification(
            slug="example-codes",
            short_name="example-codes",
            name="Canonical example",
            codes=(ResolvedClassificationCode(code="001", label="Canonical one"),),
            sentinel_codes=(SentinelCode(code="001", meaning="stale entry"),),
        )
    assert "'001'" in str(exc_info.value)


@pytest.mark.parametrize("status", ["extended"])
def test_classifications_and_explicit_conformance_are_written_exactly(
    tmp_path: Path,
    status: str,
) -> None:
    classification = _classification()
    predecessor = _classification("older-codes")
    succession = ResolvedClassificationSuccession(
        predecessor=predecessor.slug,
        successor=classification.slug,
        effective_year=2000,
        note="curated:fixture",
    )
    conformance = ResolvedConformance.model_validate(
        {
            "declared_classification": classification.slug,
            "status": status,
            "checked_codes": ("", "001", "missing"),
            "nonconforming_members": (
                ("", "Unspecified"),
                ("missing", "Unlisted label"),
            ),
        }
    )
    variable = _variable()
    state = variable.states[0].model_copy(
        update={
            "value_set": ResolvedCodeSet(
                members=(
                    ("001", "Observed label"),
                    ("missing", "Unlisted label"),
                    ("", "Unspecified"),
                )
            ),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=classification.slug, conformance=conformance
                ),
            ),
        }
    )
    variable = variable.model_copy(update={"states": (state,)})
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (variable,),
        output,
        manifest={},
        classifications=(classification, predecessor),
        classification_successions=(succession,),
    )
    original = output.read_bytes()
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT c.short_name, c.name, c.name_en, c.publisher, c.valid_from, c.valid_to, "
                "c.description, c.url, p.slug, c.code_count, c.valid_code_count FROM classification c "
                "LEFT JOIN classification p ON p.id=c.supersedes_id WHERE c.slug=?",
                (classification.slug,),
            ).fetchone()
        ) == (
            classification.short_name,
            classification.name,
            classification.name_en,
            classification.publisher,
            2000,
            2020,
            classification.description,
            classification.url,
            predecessor.slug,
            2,
            2,
        )
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT code, label, level, is_valid FROM classification_code cc "
                "JOIN classification c ON c.id=cc.classification_id JOIN value_code vc USING(code_id) "
                "WHERE c.slug=? ORDER BY code",
                (classification.slug,),
            )
        ] == [("001", "Canonical one", 1, 1), ("002", "Canonical two", 2, 1)]
        assert tuple(
            conn.execute(
                "SELECT c.slug, status, checked_code_count, matched_code_count, nonconforming_code_count, overlap "
                "FROM classification_conformance cc JOIN classification c ON c.id=cc.declared_classification_id"
            ).fetchone()
        ) == (classification.slug, status, 3, 1, 2, 1 / 3)
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT code, label FROM classification_conformance_code JOIN value_code USING(code_id) ORDER BY code, label"
            )
        ] == [("", "Unspecified"), ("missing", "Unlisted label")]
        assert conn.execute(
            "SELECT c.slug FROM variable_state s JOIN state_classification sc USING(state_id) JOIN classification c ON c.id=sc.classification_id"
        ).fetchone()[0] == (classification.slug)
        assert conn.execute("SELECT count(*) FROM code_variable_map").fetchone()[0] == 3
        # Source and official labels can differ; membership is checked by code.
        canonical = conn.execute(
            "SELECT vc.code, vc.label FROM value_set_member m "
            "JOIN value_code vc USING(code_id) "
            "WHERE m.value_set_id=(SELECT value_set_id FROM variable_state) "
            "AND vc.code IN (SELECT ccvc.code FROM classification_code cc "
            "JOIN value_code ccvc ON ccvc.code_id=cc.code_id "
            "JOIN classification c ON c.id=cc.classification_id WHERE c.slug=?)",
            (classification.slug,),
        ).fetchall()
        extensions = conn.execute(
            "SELECT vc.code, vc.label FROM classification_conformance_code cc "
            "JOIN value_code vc USING(code_id) JOIN classification c ON c.id=cc.declared_classification_id WHERE c.slug=?",
            (classification.slug,),
        ).fetchall()
        assert {row[0] for row in canonical}.isdisjoint(row[0] for row in extensions)
        assert state.value_set is not None
        assert sorted(tuple(row) for row in (*canonical, *extensions)) == sorted(
            state.value_set.members
        )

    write_resolved_catalog(
        (variable,),
        output,
        manifest={},
        classifications=(predecessor, classification),
        classification_successions=(succession,),
    )
    assert output.read_bytes() == original


@pytest.mark.parametrize("defect", ["missing", "duplicate", "conformance"])
def test_bad_classification_references_or_membership_preserve_previous_catalog(
    tmp_path: Path,
    defect: str,
) -> None:
    classification = _classification()
    classifications = (classification,)
    variable = _variable()
    if defect == "missing":
        variable = variable.model_copy(
            update={
                "states": (
                    variable.states[0].model_copy(
                        update={
                            "classification_links": (
                                ResolvedClassificationLink(classification="missing"),
                            )
                        }
                    ),
                )
            }
        )
    elif defect == "duplicate":
        classifications = (classification, classification)
    else:
        variable = variable.model_copy(
            update={
                "states": (
                    variable.states[0].model_copy(
                        update={
                            "value_set": ResolvedCodeSet(members=(("999", "Missing"),)),
                            "classification_links": (
                                ResolvedClassificationLink(
                                    classification=classification.slug,
                                    conformance=ResolvedConformance(
                                        declared_classification=classification.slug,
                                        status="conforming",
                                        checked_codes=("999",),
                                    ),
                                ),
                            ),
                        }
                    ),
                )
            }
        )
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError, match="classification|conformance"):
        write_resolved_catalog(
            (variable,), output, manifest={}, classifications=classifications
        )
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize("effective_year", [None, 2050])
def test_cyclic_classification_succession_preserves_previous_catalog(
    tmp_path: Path, effective_year: int | None
) -> None:
    first, second = _classification("first-codes"), _classification("second-codes")
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    edges = tuple(
        ResolvedClassificationSuccession(
            predecessor=predecessor.slug,
            successor=successor.slug,
            effective_year=effective_year,
        )
        for predecessor, successor in ((first, second), (second, first))
    )
    with pytest.raises(ValueError, match="cyclic classification succession"):
        write_resolved_catalog(
            (_variable(),),
            output,
            manifest={},
            classifications=(first, second),
            classification_successions=edges,
        )
    assert output.read_bytes() == b"previous"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["existing.db"]


def test_classification_predecessor_is_a_deterministic_projection_of_active_edges(
    tmp_path: Path,
) -> None:
    books = tuple(_classification(slug) for slug in ("a", "b", "current", "future"))
    edges = (
        ResolvedClassificationSuccession(
            predecessor="a", successor="current", note="undated declaration"
        ),
        ResolvedClassificationSuccession(
            predecessor="b",
            successor="current",
            effective_year=CLASSIFICATION_SUCCESSION_AS_OF_YEAR,
        ),
        ResolvedClassificationSuccession(
            predecessor="current",
            successor="future",
            effective_year=CLASSIFICATION_SUCCESSION_AS_OF_YEAR + 1,
        ),
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (_variable(),),
        output,
        manifest={},
        classifications=books,
        classification_successions=edges,
    )
    original = output.read_bytes()
    with closing(open_built_db(output)) as conn:
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT c.slug, p.slug FROM classification c "
                "LEFT JOIN classification p ON p.id=c.supersedes_id ORDER BY c.slug"
            )
        ] == [("a", None), ("b", None), ("current", "a"), ("future", None)]
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT predecessor_slug, successor_slug, effective_year, note "
                "FROM classification_replaced_by ORDER BY predecessor_slug, successor_slug"
            )
        ] == [
            (edge.predecessor, edge.successor, edge.effective_year, edge.note)
            for edge in edges
        ]
    write_resolved_catalog(
        (_variable(),),
        output,
        manifest={},
        classifications=tuple(reversed(books)),
        classification_successions=tuple(reversed(edges)),
    )
    assert output.read_bytes() == original


@pytest.mark.parametrize("defect", ["predecessor", "successor", "duplicate"])
def test_invalid_classification_edges_fail_before_creating_output(
    tmp_path: Path, defect: str
) -> None:
    edges = (
        ResolvedClassificationSuccession(
            predecessor="missing" if defect == "predecessor" else "before",
            successor="missing" if defect == "successor" else "after",
        ),
    )
    if defect == "duplicate":
        edges += (edges[0].model_copy(update={"effective_year": 2050}),)
    with pytest.raises(ValueError, match="classification succession"):
        write_resolved_catalog(
            (_variable(),),
            tmp_path / "diagnostic.db",
            manifest={},
            diagnostic=True,
            classifications=(_classification("before"), _classification("after")),
            classification_successions=edges,
        )
    assert list(tmp_path.iterdir()) == []


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
    write_resolved_catalog((variable,), output, manifest={})
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
        manifest={},
    )
    assert output.read_bytes() == original


def test_parallel_resolved_columns_keep_distinct_coding_states(
    tmp_path: Path,
) -> None:
    variable = _variable()
    state = variable.states[0]
    states = tuple(
        state.model_copy(
            update={
                "value_set_version_label": version,
                "delivery_column_name": f"Column_{index}",
                "value_set": ResolvedCodeSet(members=(("01", version),)),
            }
        )
        for index, version in enumerate(("Edition A", "Edition B"))
    )
    variable = variable.model_copy(update={"states": states})
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest={})
    with closing(open_built_db(output)) as conn:
        rows = conn.execute(
            "SELECT value_set_version_label FROM variable_state ORDER BY value_set_version_label"
        ).fetchall()
        assert [row[0] for row in rows] == ["Edition A", "Edition B"]
        assert (
            conn.execute(
                "SELECT COUNT(DISTINCT state_id) FROM variable_state"
            ).fetchone()[0]
            == 2
        )

    conflicting = variable.model_copy(
        update={
            "states": tuple(
                s.model_copy(update={"delivery_column_name": "SameColumn"})
                for s in states
            )
        }
    )
    previous = output.read_bytes()
    with pytest.raises(ValueError, match="overlapping distinct-value_set"):
        write_resolved_catalog((conflicting,), output, manifest={})
    assert output.read_bytes() == previous


def _coded(label: str, code: str) -> dict[str, object]:
    return {
        "value_set_version_label": label,
        "value_set": ResolvedCodeSet(members=((code, label),)),
    }


@pytest.mark.parametrize(
    ("updates", "code"),
    [
        ((_coded("A", "01"), _coded("B", "02")), "overlapping_distinct_value_sets"),
        (({}, _coded("A", "01")), "overlapping_codeless_codebearing_states"),
        (
            ({}, {"pooled": True, "value_set_version_label": "P"}),
            "overlapping_pooled_explicit_states",
        ),
        # One code list under two labels is a representation, not a conflict.
        (
            (_coded("A", "01"), {**_coded("A", "01"), "value_set_version_label": "B"}),
            None,
        ),
    ],
)
def test_formation_attributes_exactly_the_column_overlaps_the_writer_refuses(
    tmp_path: Path, updates: tuple[dict[str, object], ...], code: str | None
) -> None:
    state = _state(2000)
    variable = _variable().model_copy(
        update={"states": tuple(state.model_copy(update=u) for u in updates)}
    )
    overlaps = column_state_overlaps(variable)
    output = tmp_path / "reg_meta.db"
    if code is None:
        assert overlaps == ()
        write_resolved_catalog((variable,), output, manifest={})
        return
    ((found, message),) = overlaps
    assert found == code
    with pytest.raises(ValueError) as failure:
        write_resolved_catalog((variable,), output, manifest={})
    assert message.split(": ")[0] in str(failure.value)


@pytest.mark.parametrize("end", ["2021-02-29", "9999-01-01"])
def test_alias_windows_require_real_calendar_dates(end: str) -> None:
    with pytest.raises(ValueError):
        ResolvedAliasWindow(valid_from="2021-02-01", valid_to=end)


def test_conflicting_alias_windows_fail_before_output(tmp_path: Path) -> None:
    variable = _variable()
    window = ResolvedAliasWindow(valid_from="2000-01-01", valid_to="2000-12-31")
    alias = ResolvedAlias(
        variant=variable.states[0].variant,
        delivery_column_name="Alias",
        windows=(window,),
    )
    malformed = alias.model_copy(update={"windows": (window, window)})
    output = tmp_path / "reg_meta.db"
    with pytest.raises(ValueError, match="overlapping windows"):
        write_resolved_catalog(
            (variable.model_copy(update={"aliases": (malformed,)}),),
            output,
            manifest={},
        )
    assert not output.exists()


@pytest.mark.parametrize(("sensitive", "identifier"), [(True, False), (False, True)])
def test_flags_preserve_explicit_booleans(
    tmp_path: Path, sensitive: bool, identifier: bool
) -> None:
    variable = _variable().model_copy(
        update={"is_sensitive": sensitive, "is_identifier": identifier}
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest={})
    with closing(open_built_db(output)) as conn:
        row = conn.execute(
            "SELECT is_sensitive, is_identifier FROM variable"
        ).fetchone()
        assert tuple(row) == (sensitive, identifier)


@pytest.mark.parametrize("field", ["is_sensitive", "is_identifier"])
@pytest.mark.parametrize("diagnostic", [False, True])
def test_unknown_flags_are_preserved_as_evidence_but_refused_by_writer(
    tmp_path: Path, field: str, diagnostic: bool
) -> None:
    variable = ResolvedVariable.model_validate(_variable().model_dump() | {field: None})
    assert getattr(variable, field) is None
    output = tmp_path / "diagnostic.db" if diagnostic else tmp_path / "reg_meta.db"
    with pytest.raises(ValueError, match=f"unknown flags.*{field}"):
        validate_resolved_variables((variable,))
    with pytest.raises(ValueError, match=f"unknown flags.*{field}"):
        write_resolved_catalog((variable,), output, manifest={}, diagnostic=diagnostic)
    assert not output.exists()


def test_diagnostic_artifact_is_create_only_and_builder_cannot_publish_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    normal = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), normal, manifest={})
    original = normal.read_bytes()
    diagnostic = tmp_path / "diagnostic.db"
    write_resolved_catalog((_variable(),), diagnostic, manifest={}, diagnostic=True)
    diagnostic_bytes = diagnostic.read_bytes()
    with pytest.raises(RegMetaError) as rejected:
        publish_db(diagnostic, normal)
    assert rejected.value.code == "catalog_not_publishable"
    assert normal.read_bytes() == original
    assert diagnostic.read_bytes() == diagnostic_bytes
    assert not normal.with_name("reg_meta.db.prev").exists()
    for destination in (normal, diagnostic):
        with pytest.raises(ValueError, match="new explicit path"):
            write_resolved_catalog(
                (_variable(),), destination, manifest={}, diagnostic=True
            )
    active = tmp_path / "absent-active"
    monkeypatch.setenv("REG_META_DB", str(active))
    with pytest.raises(ValueError, match="new explicit path"):
        write_resolved_catalog(
            (_variable(),), active / "reg_meta.db", manifest={}, diagnostic=True
        )
    assert not active.exists()


def test_diagnostic_structural_failure_never_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def failed_validation(path: Path, *, corpus: bool) -> ValidationResult:
        assert corpus is False
        result = ValidationResult()
        result.fail("synthetic structural failure")
        return result

    monkeypatch.setattr(resolved_catalog, "validate_built_db", failed_validation)
    output = tmp_path / "diagnostic.db"
    with pytest.raises(ValueError, match="synthetic structural failure"):
        write_resolved_catalog((_variable(),), output, manifest={}, diagnostic=True)
    assert not output.exists()


def test_diagnostic_publication_cannot_overwrite_a_racing_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "diagnostic.db"

    def competing_publication(path: Path, *, corpus: bool) -> ValidationResult:
        result = validate_built_db(path, corpus=corpus)
        assert result.passed, result.format_report()
        output.write_bytes(b"published concurrently")
        return result

    monkeypatch.setattr(resolved_catalog, "validate_built_db", competing_publication)
    with pytest.raises(FileExistsError):
        write_resolved_catalog((_variable(),), output, manifest={}, diagnostic=True)
    assert output.read_bytes() == b"published concurrently"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["diagnostic.db"]


@pytest.mark.parametrize("field", ["is_sensitive", "is_identifier"])
@pytest.mark.parametrize("value", [0, 1, "false", "true"])
def test_malformed_flags_fail_shared_preflight_before_publication(
    tmp_path: Path, field: str, value: int | str
) -> None:
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), output, manifest={})
    original = output.read_bytes()
    variable = _variable().model_copy(update={field: value})
    with pytest.raises(ValidationError, match=field):
        validate_resolved_variables((variable,))
    with pytest.raises(ValidationError, match=field):
        write_resolved_catalog((variable,), output, manifest={})
    assert output.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


def test_documented_codes_do_not_require_known_physical_type(tmp_path: Path) -> None:
    members = (
        ("01", "Participation"),
        ("1", "Another code"),
        ("", "Undocumented value"),
        ("01", "Another label"),
        ("01", "Participation"),
        (" 01", "Participation"),
    )
    code_set = ResolvedCodeSet(members=members)
    state = _state(2000).model_copy(
        update={"value_set": code_set, "data_type": None, "data_length": None}
    )
    variable = _variable().model_copy(update={"states": (state, _state(2002))})
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest={})
    with closing(open_built_db(output)) as conn:
        state = conn.execute(
            "SELECT * FROM variable_state WHERE valid_from='2000-01-01'"
        ).fetchone()
        assert (state["data_type"], state["data_length"]) == (None, None)
        assert state["value_set_id"] is not None
        assert tuple(
            tuple(row)
            for row in conn.execute(
                "SELECT code, label FROM value_set_member JOIN value_code USING (code_id) WHERE value_set_id=? ORDER BY code, label",
                (state["value_set_id"],),
            )
        ) == tuple(sorted(set(members)))
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM variable_state WHERE valid_from='2001-01-01'"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT value_set_id FROM variable_state WHERE valid_from='2002-01-01'"
            ).fetchone()[0]
            is None
        )
        hits = conn.execute(
            "SELECT c.code, c.mapping_count FROM value_code_fts f JOIN value_code c ON c.code_id=f.rowid WHERE value_code_fts MATCH 'Participation'"
        ).fetchall()
        assert len(hits) == 2
        assert {row["code"] for row in hits} == {"01", " 01"}
        assert all(row["mapping_count"] == 1 for row in hits)


def test_shared_memberships_have_stable_ids_and_deterministic_replay(
    tmp_path: Path,
) -> None:
    members = (("01", "Participation"), ("02", "Employment"))
    variables = tuple(
        _variable(provider).model_copy(
            update={
                "states": tuple(
                    _state(year).model_copy(
                        update={"value_set": ResolvedCodeSet(members=members)}
                    )
                    for year in (2000, 2002)
                )
            }
        )
        for provider in ("scb", "sos")
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(variables, output, manifest={})
    original = output.read_bytes()
    with closing(open_built_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM value_set").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM value_code").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM value_set_member").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM code_variable_map").fetchone()[0] == 4
        assert {
            row[0] for row in conn.execute("SELECT mapping_count FROM value_code")
        } == {2}
        assert (
            conn.execute(
                "SELECT count(DISTINCT value_set_id) FROM variable_state"
            ).fetchone()[0]
            == 1
        )
        shared_id = conn.execute("SELECT value_set_id FROM value_set").fetchone()[0]
    reordered = tuple(
        variable.model_copy(
            update={
                "states": tuple(
                    state.model_copy(
                        update={
                            "value_set": ResolvedCodeSet(
                                members=tuple(reversed(members)) + members
                            )
                        }
                    )
                    for state in reversed(variable.states)
                )
            }
        )
        for variable in reversed(variables)
    )
    write_resolved_catalog(reordered, output, manifest={})
    assert output.read_bytes() == original
    # Adding an unrelated membership cannot renumber an existing content ID.
    other = _variable(slug="other").model_copy(
        update={
            "states": (
                _state(2000).model_copy(
                    update={"value_set": ResolvedCodeSet(members=(("0", "Other"),))}
                ),
            )
        }
    )
    write_resolved_catalog((other, *variables), output, manifest={})
    with closing(open_built_db(output)) as conn:
        ids = conn.execute(
            "SELECT DISTINCT state.value_set_id FROM variable_state state "
            "JOIN variable USING (variable_id) WHERE slug = 'ampoltyp'"
        ).fetchall()
        assert [row[0] for row in ids] == [shared_id]


def test_distinct_memberships_and_changed_labels_remain_distinct(
    tmp_path: Path,
) -> None:
    memberships = (
        (("01", "Participation"),),
        (("01", "Revised label"),),
        (("01", "Participation"), ("02", "Employment")),
    )
    variable = _variable().model_copy(
        update={
            "states": tuple(
                _state(year).model_copy(
                    update={"value_set": ResolvedCodeSet(members=members)}
                )
                for year, members in zip(range(2000, 2003), memberships, strict=True)
            )
        }
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest={})
    with closing(open_built_db(output)) as conn:
        states = conn.execute(
            "SELECT value_set_id FROM variable_state ORDER BY valid_from"
        ).fetchall()
        assert len({row[0] for row in states}) == 3
        for state, members in zip(states, memberships, strict=True):
            assert (
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        "SELECT code, label FROM value_set_member JOIN value_code USING (code_id) WHERE value_set_id=? ORDER BY code, label",
                        (state[0],),
                    )
                )
                == members
            )
    assert validate_built_db(output, corpus=False).passed


@pytest.mark.parametrize(
    "members",
    [(), ((1, "Label"),), (("01", None),), (("01", "Label", "Extra"),)],
)
def test_malformed_memberships_fail_shared_preflight_and_preserve_catalog(
    tmp_path: Path, members: tuple
) -> None:
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), output, manifest={})
    original = output.read_bytes()
    malformed = ResolvedCodeSet(members=(("01", "Label"),)).model_copy(
        update={"members": members}
    )
    variable = _variable().model_copy(
        update={"states": (_state(2000).model_copy(update={"value_set": malformed}),)}
    )
    with pytest.raises(ValidationError):
        validate_resolved_variables((variable,))
    with pytest.raises(ValidationError):
        write_resolved_catalog((variable,), output, manifest={})
    assert output.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


@pytest.mark.parametrize(
    "update",
    [
        {"valid_from": "2000"},
        {"valid_from": "20000101"},
        {"valid_from": "2000-02-30"},
        {"valid_from": "2001-01-01"},
        {"valid_to": "9999-01-01"},
        {"delivery_column_name": " AmPolTyp"},
    ],
)
def test_invalid_state_boundaries_are_rejected(update: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        ResolvedState.model_validate(_state(2000).model_dump() | update)


def test_identity_and_state_ambiguity_are_rejected() -> None:
    with pytest.raises(ValidationError, match="overlapping"):
        ResolvedVariable.model_validate(
            _variable().model_dump() | {"states": (_state(2000), _state(2000))}
        )
    with pytest.raises(ValidationError, match="slug"):
        ResolvedVariable.model_validate(_variable().model_dump() | {"slug": "Bad_slug"})
    with pytest.raises(ValidationError, match="is_sensitive"):
        ResolvedVariable.model_validate(
            {k: v for k, v in _variable().model_dump().items() if k != "is_sensitive"}
        )
    with pytest.raises(ValidationError, match="reserved"):
        ResolvedVariable.model_validate(_variable().model_dump() | {"slug": "_default"})


@pytest.mark.parametrize("conflict", ["register", "variant", "variable", "manifest"])
def test_conflicts_preserve_previous_catalog(tmp_path: Path, conflict: str) -> None:
    output = tmp_path / "reg_meta.db"
    variable = _variable()
    write_resolved_catalog((variable,), output, manifest={})
    original = output.read_bytes()
    other = _variable(slug="another")
    metadata = {}
    if conflict == "register":
        other = other.model_copy(
            update={
                "register_ref": variable.register_ref.model_copy(
                    update={"name": "Other"}
                )
            }
        )
    elif conflict == "variant":
        other = other.model_copy(
            update={
                "states": tuple(
                    state.model_copy(
                        update={
                            "variant": state.variant.model_copy(
                                update={"name": "Other"}
                            )
                        }
                    )
                    for state in other.states
                )
            }
        )
    elif conflict == "variable":
        other = variable
    else:
        metadata = {"schema_version": "invalid"}
    if conflict != "manifest":
        with pytest.raises(ValueError, match="inconsistent|duplicate"):
            validate_resolved_variables((variable, other))
    with pytest.raises(ValueError, match="inconsistent|duplicate|manifest"):
        write_resolved_catalog((variable, other), output, manifest=metadata)
    assert output.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


def test_shared_preflight_rejects_invalid_contracts_and_unknown_providers() -> None:
    with pytest.raises(ValueError, match="empty"):
        validate_resolved_variables(())
    invalid = _variable().model_copy(update={"states": ()})
    with pytest.raises(ValidationError, match="at least one delivery state"):
        validate_resolved_variables((invalid,))
    with pytest.raises(RegMetaError) as error:
        validate_resolved_variables((_variable("unknown-provider"),))
    assert error.value.code == "unknown_provider"
    assert "No provider_id seed" in error.value.message


@pytest.mark.parametrize("corpus", [False, True])
def test_failed_validation_preserves_previous_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corpus: bool
) -> None:
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), output, manifest={})
    original = output.read_bytes()

    expected_corpus = corpus

    def fail_validation(path: Path, *, corpus: bool) -> ValidationResult:
        assert path != output
        assert corpus is expected_corpus
        assert validate_built_db(path, corpus=False).passed
        result = ValidationResult()
        result.fail("synthetic publication gate failure")
        return result

    monkeypatch.setattr(resolved_catalog, "validate_built_db", fail_validation)
    with pytest.raises(ValueError, match="synthetic publication gate failure"):
        write_resolved_catalog((_variable("sos"),), output, manifest={}, corpus=corpus)
    assert output.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


@pytest.mark.parametrize(
    "updates",
    [
        {"period_scope": "year_independent", "valid_from": "2000-01-01"},
        {"period_scope": "year_independent", "valid_to": "9999-12-31"},
        {"period_scope": "year_independent", "pooled": True},
        {"period_scope": "intervals"},
        {"period_scope": "intervals", "valid_from": "2000-01-01"},
    ],
)
def test_year_independent_state_requires_no_dates_and_dated_state_requires_both(
    updates,
):
    state = _state(2000).model_dump()
    state.update(valid_from=None, valid_to=None)
    state.update(updates)
    with pytest.raises(ValidationError):
        ResolvedState.model_validate(state)


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
    write_resolved_catalog((variable,), output, manifest={})
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
            write_resolved_catalog((bad,), tmp_path / "bad.db", manifest={})
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
            manifest={},
        )


def _scoped_sentinel_variable():
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.resolved_catalog import ResolvedScopedSentinels

    book = _classification()
    certificate = ResolvedScopedSentinels(
        delivery_column_name="AmPolTyp",
        valid_from="2000-01-01",
        valid_to="2000-12-31",
        classification_sha256=canonical_sha256(book.model_dump(mode="json")),
        source_fingerprints=("a" * 64,),
        members=(("09350", "Okänt"),),
        provenance="checked-source-sentinel: exact original coding and codebook guards",
    )
    conformance = ResolvedConformance(
        declared_classification=book.slug,
        status="extended",
        checked_codes=("001", "09350"),
        sentinel_members=certificate.members,
        scoped_sentinels=(certificate,),
    )
    state = _state(2000).model_copy(
        update={
            "value_set": ResolvedCodeSet(
                members=(("001", "Source label"), ("09350", "Okänt"))
            ),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=book.slug, conformance=conformance
                ),
            ),
        }
    )
    return _variable().model_copy(update={"states": (state,)}), book


def test_scoped_sentinel_certificate_keeps_local_member_without_changing_book(
    tmp_path: Path,
) -> None:
    variable, book = _scoped_sentinel_variable()
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest={}, classifications=(book,))
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT status, checked_code_count, matched_code_count, nonconforming_code_count FROM classification_conformance"
            ).fetchone()
        ) == ("extended", 2, 1, 1)
        assert (
            conn.execute(
                "SELECT count(*) FROM classification_code WHERE code_id IN (SELECT code_id FROM value_code WHERE code='09350')"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM value_code WHERE code='09350' AND label='Okänt'"
            ).fetchone()[0]
            == 1
        )


@pytest.mark.parametrize(
    "defect", ["missing", "book", "column", "window", "label", "canonical"]
)
def test_scoped_sentinel_certificate_tampering_is_refused_before_publish(
    tmp_path: Path, defect: str
) -> None:
    variable, book = _scoped_sentinel_variable()
    state = variable.states[0]
    conformance = state.classification_links[0].conformance
    assert conformance is not None
    certificate = conformance.scoped_sentinels[0]
    if defect == "missing":
        conformance = conformance.model_copy(update={"scoped_sentinels": ()})
    else:
        update = {
            "book": {"classification_sha256": "b" * 64},
            "column": {"delivery_column_name": "Other"},
            "window": {"valid_to": "2000-06-30"},
            "label": {"members": (("09350", "Different meaning"),)},
            "canonical": {"members": (("001", "Source label"),)},
        }[defect]
        conformance = conformance.model_copy(
            update={"scoped_sentinels": (certificate.model_copy(update=update),)}
        )
    variable = variable.model_copy(
        update={
            "states": (
                state.model_copy(
                    update={
                        "classification_links": (
                            state.classification_links[0].model_copy(
                                update={"conformance": conformance}
                            ),
                        )
                    }
                ),
            )
        }
    )
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    with pytest.raises(
        ValueError, match="scoped sentinel certificate|conformance disagrees"
    ):
        write_resolved_catalog(
            (variable,), output, manifest={}, classifications=(book,)
        )
    assert output.read_bytes() == b"previous"


def test_scoped_sentinel_certificate_requires_positive_source_and_case_evidence() -> (
    None
):
    variable, _ = _scoped_sentinel_variable()
    certificate = (
        variable.states[0].classification_links[0].conformance.scoped_sentinels[0]
    )
    for field, invalid in (("source_fingerprints", ()), ("provenance", "")):
        with pytest.raises(ValidationError):
            type(certificate).model_validate(
                {**certificate.model_dump(), field: invalid}
            )


def test_data_warnings_persist_exact_ownership_without_inventing_scope(tmp_path: Path):
    from hashlib import sha256

    from reg_meta.catalog import DataWarning
    from reg_meta.source_evidence import canonical_sha256

    payload = {
        "register_fqid": "scb/example",
        "variable_fqid": "scb/example/ampoltyp",
        "variant": "individuals",
        "delivery_column_name": "AmPolTyp",
        "valid_from": "2000-01-01",
        "valid_to": "2000-06-15",
        "code": "missing_coding_period",
        "severity": "warning",
        "summary": "Historical codes unavailable",
        "detail": "No source list for this window.",
        "diagnostic_detail_sha256": sha256(
            b"No source list for this window."
        ).hexdigest(),
        "source_subject": "native:44",
        "fields": [],
        "refs": [],
        "withheld_output": ["coding"],
        "acknowledged_by": "review-44",
        "case_id": None,
    }
    warning = DataWarning.model_validate(
        {"warning_id": canonical_sha256(payload), **payload}
    )
    unscoped_payload = {
        **payload,
        "variable_fqid": None,
        "variant": None,
        "delivery_column_name": None,
        "valid_from": None,
        "valid_to": None,
    }
    unscoped = DataWarning.model_validate(
        {"warning_id": canonical_sha256(unscoped_payload), **unscoped_payload}
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (_variable(),), output, manifest={}, data_warnings=(warning, unscoped)
    )
    with closing(open_built_db(output)) as conn:
        rows = conn.execute("SELECT * FROM data_warning ORDER BY warning_id").fetchall()
        assert {
            DataWarning.model_validate_json(row["warning_json"]) for row in rows
        } == {warning, unscoped}
        scoped = next(row for row in rows if row["warning_id"] == warning.warning_id)
        assert (
            scoped["valid_from"],
            scoped["valid_to"],
            scoped["delivery_column_name"],
        ) == (warning.valid_from, warning.valid_to, warning.delivery_column_name)
        assert (
            scoped["variable_id"]
            == conn.execute("SELECT variable_id FROM variable").fetchone()[0]
        )
        assert (
            scoped["register_variant_id"]
            == conn.execute(
                "SELECT register_variant_id FROM register_variant"
            ).fetchone()[0]
        )
        global_row = next(
            row for row in rows if row["warning_id"] == unscoped.warning_id
        )
        assert all(
            global_row[field] is None
            for field in (
                "variable_id",
                "register_variant_id",
                "delivery_column_name",
                "valid_from",
                "valid_to",
            )
        )
        assert global_row["register_id"] == scoped["register_id"]
    for label, state in (
        ("open", _state(2000).model_copy(update={"valid_to": "9999-12-31"})),
        (
            "independent",
            _state(2000).model_copy(
                update={
                    "period_scope": "year_independent",
                    "valid_from": None,
                    "valid_to": None,
                }
            ),
        ),
    ):
        candidate = tmp_path / f"{label}.db"
        write_resolved_catalog(
            (_variable().model_copy(update={"states": (state,)}),),
            candidate,
            manifest={},
            data_warnings=(warning, unscoped),
        )
        with closing(open_built_db(candidate)) as conn:
            assert {
                DataWarning.model_validate_json(row[0])
                for row in conn.execute("SELECT warning_json FROM data_warning")
            } == {warning, unscoped}
            assert tuple(
                conn.execute(
                    "SELECT valid_from, valid_to, period_scope FROM variable_state"
                ).fetchone()
            ) == (state.valid_from, state.valid_to, state.period_scope)
    with pytest.raises(ValidationError, match="identity"):
        write_resolved_catalog(
            (_variable(),),
            tmp_path / "tampered.db",
            manifest={},
            data_warnings=(warning.model_copy(update={"detail": "Changed"}),),
        )


def test_warning_ownership_requires_complete_source_and_delivery_witnesses():
    from hashlib import sha256
    from types import SimpleNamespace
    from typing import cast

    from reg_meta.source_evidence import SourceRecordRef
    from reg_meta_build.data_warnings import scope_data_warnings
    from reg_meta_build.source_curation import ResolutionDiagnostic
    from reg_meta_build.source_records import ScopeInterval, TemporalScope

    variable = _variable()
    ref = SourceRecordRef(source="fixture", semantic_record_key=("44",))
    occurrence = SimpleNamespace(
        variable_key=("44",),
        variant_key=("v",),
        corrections=(),
        source_records=(),
        column_key=None,
        edition_scope=TemporalScope(kind="not_applicable"),
        edition_period_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2000", end="2000"),)
        ),
        fields=SimpleNamespace(
            column_name=SimpleNamespace(status="value", value="AmPolTyp")
        ),
        evidence=(
            SimpleNamespace(
                source="fixture",
                locators=(SimpleNamespace(semantic_record_key=("44",)),),
            ),
        ),
    )
    issue = ResolutionDiagnostic(
        code="missing_coding_period",
        severity="warning",
        subject="44",
        detail="Domain unavailable",
        refs=(ref,),
        valid_from="2000-01-01",
        valid_to="2000-12-31",
        acknowledged_by="accepted",
    )
    scope = SimpleNamespace(
        variables={("44",): variable},
        corrections=SimpleNamespace(occurrences=(occurrence,)),
        parents=SimpleNamespace(
            registers={("r",): variable.register_ref},
            variants={("v",): variable.states[0].variant},
        ),
        diagnostics=(issue,),
        evaluations=(),
    )
    (warning,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert warning.detail != issue.detail
    assert warning.diagnostic_detail_sha256 == sha256(issue.detail.encode()).hexdigest()
    assert str(warning.variable_fqid) == "scb/example/ampoltyp"
    assert warning.variant == "individuals"
    assert warning.delivery_column_name == "AmPolTyp"
    large_detail = "Original source label Å " * 100_000
    scope.diagnostics = (
        issue.model_copy(
            update={"code": "item_validity_set_aside", "detail": large_detail}
        ),
    )
    (compact,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert len(compact.detail) < 300
    assert (
        compact.diagnostic_detail_sha256
        == sha256(large_detail.encode("utf-8")).hexdigest()
    )
    scope.diagnostics = (
        issue.model_copy(
            update={
                "refs": (
                    SourceRecordRef(source="fixture", semantic_record_key=("missing",)),
                )
            }
        ),
    )
    (warning,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert warning.variable_fqid is None and warning.variant is None
    scope.diagnostics = (
        issue.model_copy(update={"valid_from": None, "valid_to": None}),
    )
    (warning,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert warning.variable_fqid is not None and warning.variant is None
    scope.diagnostics = (
        issue.model_copy(
            update={"code": "metadata_projected", "acknowledged_by": None}
        ),
    )
    assert scope_data_warnings(cast("ScopeResolution", scope)) == ()


@pytest.mark.parametrize(
    ("fields", "code"),
    [
        (("data_type",), "assumed_storage_type"),
        (("identity",), "source_identity_assumption"),
        (("coding",), "response_domain_assumption"),
        (("availability",), "source_availability_limitation"),
    ],
)
def test_assumption_warns_on_added_physical_column_not_anchor_identity(fields, code):
    from types import SimpleNamespace
    from typing import cast

    from reg_meta_build.data_warnings import scope_data_warnings
    from reg_meta_build.source_records import ScopeInterval, TemporalScope

    variable = _variable()
    scope = SimpleNamespace(
        variables={("graft",): variable},
        parents=SimpleNamespace(
            registers={("r",): variable.register_ref},
            variants={("v",): variable.states[0].variant},
        ),
        diagnostics=(),
        evaluations=(
            SimpleNamespace(
                case_id="graft-case",
                status="applicable",
                decision=SimpleNamespace(
                    kind="correct_occurrences",
                    data_warning="Historical storage type is assumed",
                    data_warning_refs=(),
                    data_warning_fields=fields,
                    reason="Historical text assumption",
                    provenance="Exact reviewed errata-column authority",
                ),
            ),
        ),
        corrections=SimpleNamespace(
            occurrences=(
                SimpleNamespace(
                    variable_key=("graft",),
                    variant_key=("v",),
                    occurrence_key="added-column",
                    column_key=None,
                    edition_scope=TemporalScope(kind="not_applicable"),
                    edition_period_scope=TemporalScope(
                        kind="intervals",
                        intervals=(ScopeInterval(start="2000", end="2000"),),
                    ),
                    fields=SimpleNamespace(
                        column_name=SimpleNamespace(status="value", value="AmPolTyp")
                    ),
                    evidence=(),
                    source_records=(),
                    corrections=(
                        SimpleNamespace(
                            case_id="graft-case",
                            provenance="maintainer-authorized storage-type assumption: historical text",
                        ),
                    ),
                ),
            )
        ),
    )
    (warning,) = scope_data_warnings(cast("ScopeResolution", scope))
    assert str(warning.variable_fqid) == "scb/example/ampoltyp"
    assert warning.code == code and warning.refs == ()
    assert warning.source_subject == "graft-case" and warning.case_id == "graft-case"
    assert (
        warning.valid_from == "2000-01-01"
        and warning.delivery_column_name == "AmPolTyp"
    )


def test_incomplete_classification_partition_preserves_previous_catalog(
    tmp_path: Path,
) -> None:
    book = _classification()
    variable = _variable()
    conformance = ResolvedConformance(
        declared_classification=book.slug,
        status="conforming",
        checked_codes=("001",),
    )
    state = variable.states[0].model_copy(
        update={
            "value_set": ResolvedCodeSet(
                members=(("001", "Source label"), ("", "Source missing"))
            ),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=book.slug, conformance=conformance
                ),
            ),
        }
    )
    output = tmp_path / "reg_meta.db"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError, match="conformance must check every distinct code"):
        write_resolved_catalog(
            (variable.model_copy(update={"states": (state,)}),),
            output,
            manifest={},
            classifications=(book,),
        )
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize("defect", ["duplicate", "order"])
def test_state_classification_links_refuse_duplicate_or_unsorted_books(defect):
    first = ResolvedClassificationLink(classification="first")
    second = ResolvedClassificationLink(classification="second")
    links = (first, first) if defect == "duplicate" else (second, first)
    with pytest.raises(ValueError, match="unique sorted books"):
        ResolvedState.model_validate(
            _state(2000).model_copy(update={"classification_links": links})
        )


def _classified_alias_variable():
    variable, book = _scoped_sentinel_variable()
    original = variable.states[0]
    window = ResolvedAliasWindow(
        valid_from=original.valid_from,
        valid_to=original.valid_to,
        coding_metadata="per_column",
        value_set=original.value_set,
        value_set_version_label="Original physical list",
        classification_links=original.classification_links,
    )
    backing = original.model_copy(
        update={
            "value_set": None,
            "value_set_version_label": "",
            "classification_links": (),
        }
    )
    alias = ResolvedAlias(
        variant=original.variant,
        delivery_column_name=original.delivery_column_name,
        windows=(window,),
    )
    return variable.model_copy(update={"states": (backing,), "aliases": (alias,)}), book


def test_classified_alias_keeps_own_domain_and_book_without_backing_inheritance(
    tmp_path,
):
    variable, book = _classified_alias_variable()
    output = tmp_path / "alias.db"
    write_resolved_catalog((variable,), output, manifest={}, classifications=(book,))
    with closing(open_built_db(output)) as conn:
        row = conn.execute(
            "SELECT a.delivery_column_name, c.slug, a.conformance FROM alias_window_classification a JOIN classification c ON c.id=a.classification_id"
        ).fetchone()
        assert tuple(row[:2]) == ("AmPolTyp", book.slug)
        assert (
            ResolvedConformance.model_validate_json(row[2])
            == variable.aliases[0].windows[0].classification_links[0].conformance
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM state_classification").fetchone()[0] == 0
        )
        assert (
            conn.execute("SELECT value_set_id FROM variable_state").fetchone()[0]
            is None
        )
        assert validate_built_db(output, corpus=False).passed


@pytest.mark.parametrize(
    "defect", ["wrong_book", "partial_codes", "missing_alias", "changed_member"]
)
def test_alias_conformance_sql_boundary_rejects_tampering(tmp_path, defect):
    import sqlite3

    from reg_meta_build.validate import _check_alias_classification

    variable, book = _classified_alias_variable()
    output = tmp_path / "alias.db"
    write_resolved_catalog((variable,), output, manifest={}, classifications=(book,))
    with closing(sqlite3.connect(output)) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        if defect == "missing_alias":
            conn.execute("DELETE FROM variable_alias_window")
        elif defect == "wrong_book":
            conn.execute("UPDATE classification SET slug='different-book'")
        elif defect == "partial_codes":
            conn.execute(
                "UPDATE alias_window_classification SET conformance=json_set(conformance, '$.checked_codes', json('[\"001\"]'))"
            )
        else:
            conn.execute(
                "UPDATE value_code SET label='Changed label' WHERE code='09350'"
            )
        result = ValidationResult()
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        _check_alias_classification(conn, result, tables)
        assert not result.passed
        assert any("alias classification" in failure for failure in result.failures)


def test_alias_classification_contract_refuses_shared_and_unchecked_domain():
    variable, _ = _classified_alias_variable()
    window = variable.aliases[0].windows[0]
    with pytest.raises(ValidationError, match="shared representation coding"):
        ResolvedAliasWindow.model_validate(
            window.model_dump() | {"coding_metadata": "shared"}
        )
    link = window.classification_links[0]
    assert link.conformance is not None
    with pytest.raises(ValidationError, match="every distinct code"):
        ResolvedAliasWindow.model_validate(
            window.model_dump()
            | {
                "classification_links": (
                    link.model_copy(
                        update={
                            "conformance": link.conformance.model_copy(
                                update={"checked_codes": ("001",)}
                            )
                        }
                    ),
                )
            }
        )


def test_alias_scoped_certificate_book_fingerprint_is_checked_before_writing(tmp_path):
    variable, book = _classified_alias_variable()
    alias = variable.aliases[0]
    window = alias.windows[0]
    link = window.classification_links[0]
    assert link.conformance is not None
    certificate = link.conformance.scoped_sentinels[0].model_copy(
        update={"classification_sha256": "b" * 64}
    )
    conformance = link.conformance.model_copy(
        update={"scoped_sentinels": (certificate,)}
    )
    window = window.model_copy(
        update={
            "classification_links": (
                link.model_copy(update={"conformance": conformance}),
            )
        }
    )
    variable = variable.model_copy(
        update={"aliases": (alias.model_copy(update={"windows": (window,)}),)}
    )
    output = tmp_path / "bad-certificate.db"
    with pytest.raises(ValueError, match="scoped sentinel certificate"):
        write_resolved_catalog(
            (variable,), output, manifest={}, classifications=(book,)
        )
    assert not output.exists()
