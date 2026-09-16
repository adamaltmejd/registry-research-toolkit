"""The resolved writer publishes normal catalogs without source reconciliation."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from reg_meta.catalog import Catalog, ResolvedVariable as CatalogVariable
from reg_meta.db import CLASSIFICATION_SUCCESSION_AS_OF_YEAR, open_db
from reg_meta.errors import RegMetaError
from reg_meta.queries import search
from reg_meta.search import CodeSearchResult, VariableSearchResult
from reg_meta_build.db import publish_db
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedClassification,
    ResolvedClassificationCode,
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
    validate_resolved_variables,
    write_resolved_catalog,
)
from reg_meta_build.validate import ValidationResult, validate_built_db

from reg_meta_build import resolved_catalog

if TYPE_CHECKING:
    from pathlib import Path


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


@pytest.mark.parametrize("provider", ["scb", "sos"])
@pytest.mark.parametrize("variant", ["individuals", "_default"])
def test_normal_catalog_api_search_and_structural_validation(
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

    fqid = f"{provider}/example/ampoltyp"
    catalog = Catalog.open(output.parent)
    try:
        assert catalog.resolve(f"{provider}/example").name == "Example"
        result = catalog.resolve(fqid)
        assert isinstance(result, CatalogVariable)
        assert result.name == variable.name
        assert result.definition == variable.definition
        assert result.is_sensitive is True
        assert result.is_identifier is False
        assert result.provider_key == "44"
        assert len(result.states) == 2
        assert catalog.resolve_at(fqid, 1999) == []
        assert catalog.resolve_at(fqid, 2001) == []
        assert catalog.resolve_at(fqid, 2003) == []
        state = catalog.resolve_at(fqid, 2002, variant=variant)[0]
        assert (state.valid_from, state.valid_to) == ("2002-01-01", "2002-12-31")
        assert state.delivery_column_name == "AmPolTypUpdated"
        assert (state.data_type, state.data_length) == ("integer", "8")
        assert state.operational_definition == "State definition for 2002"
        assert state.provenance == "curation:fixture:2002"
        assert state.value_set_id is None
    finally:
        catalog.close()
    with closing(open_db(output)) as conn:
        results = search(conn, "searchable", field="description", type="variable")
        assert len(results.results) == 1
        hit = results.results[0]
        assert isinstance(hit, VariableSearchResult)
        assert str(hit.fqid) == fqid
        columns = search(conn, "AmPolTypUpdated", field="datacolumn")
        assert len(columns.results) == 1
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
    with closing(open_db(output)) as conn:
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
    with closing(open_db(output)) as conn:
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


@pytest.mark.parametrize("status", ["kept", "severed"])
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
            "checked_codes": ("001", "missing"),
            "nonconforming_members": (("missing", "Unlisted label"),),
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
            "classification": classification.slug if status == "kept" else None,
            "conformance": conformance,
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
    with closing(open_db(output)) as conn:
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
        ) == (classification.slug, status, 2, 1, 1, 0.5)
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT code, label FROM classification_conformance_code JOIN value_code USING(code_id)"
            )
        ] == [("missing", "Unlisted label")]
        assert conn.execute(
            "SELECT c.slug FROM variable_state s LEFT JOIN classification c ON c.id=s.classification_id"
        ).fetchone()[0] == (classification.slug if status == "kept" else None)
        assert conn.execute("SELECT count(*) FROM code_variable_map").fetchone()[0] == 3
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
                    variable.states[0].model_copy(update={"classification": "missing"}),
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
                            "classification": classification.slug,
                            "value_set": ResolvedCodeSet(members=(("999", "Missing"),)),
                            "conformance": ResolvedConformance(
                                declared_classification=classification.slug,
                                status="kept",
                                checked_codes=("999",),
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
    with closing(open_db(output)) as conn:
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
    with closing(open_db(output)) as conn:
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
    with closing(open_db(output)) as conn:
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
    with closing(open_db(output)) as conn:
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
    catalog = Catalog.open(output.parent)
    try:
        result = catalog.resolve_at("scb/example/ampoltyp", 2000)[0]
        assert (result.data_type, result.data_length) == (None, None)
        assert result.value_set_id is not None
        assert result.value_set is not None
        assert tuple((m.code, m.label) for m in result.value_set) == tuple(
            sorted(set(members))
        )
        assert catalog.value_set_codes(result.value_set_id) == result.value_set
        # Membership belongs to this exact state; neither a missing year nor the
        # next explicitly uncoded state inherits it.
        assert catalog.resolve_at("scb/example/ampoltyp", 2001) == []
        assert catalog.resolve_at("scb/example/ampoltyp", 2002)[0].value_set is None
    finally:
        catalog.close()
    with closing(open_db(output)) as conn:
        hits = search(conn, "Participation", field="value", type="value").results
        assert len(hits) == 2
        assert all(isinstance(hit, CodeSearchResult) for hit in hits)
        assert {hit.code for hit in hits if isinstance(hit, CodeSearchResult)} == {
            "01",
            " 01",
        }
        assert all(
            hit.variable_count == 1 for hit in hits if isinstance(hit, CodeSearchResult)
        )


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
    with closing(open_db(output)) as conn:
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
    with closing(open_db(output)) as conn:
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
    catalog = Catalog.open(output.parent)
    try:
        states = tuple(
            catalog.resolve_at("scb/example/ampoltyp", year)[0]
            for year in range(2000, 2003)
        )
        assert len({state.value_set_id for state in states}) == 3
        for state, members in zip(states, memberships, strict=True):
            assert state.value_set is not None
            assert tuple((m.code, m.label) for m in state.value_set) == members
    finally:
        catalog.close()
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


def test_failed_structural_validation_preserves_previous_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), output, manifest={})
    original = output.read_bytes()

    def fail_validation(path: Path, *, corpus: bool) -> ValidationResult:
        assert path != output
        assert corpus is False
        assert validate_built_db(path, corpus=corpus).passed
        result = ValidationResult()
        result.fail("synthetic publication gate failure")
        return result

    monkeypatch.setattr(resolved_catalog, "validate_built_db", fail_validation)
    with pytest.raises(ValueError, match="synthetic publication gate failure"):
        write_resolved_catalog((_variable("sos"),), output, manifest={})
    assert output.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]
