"""The resolved writer publishes normal catalogs without source reconciliation."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from reg_meta.catalog import Catalog, ResolvedVariable as CatalogVariable
from reg_meta.db import open_db
from reg_meta.errors import RegMetaError
from reg_meta.queries import search
from reg_meta.search import VariableSearchResult
from reg_meta_build.resolved_catalog import (
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
def test_normal_catalog_api_search_and_structural_validation(
    tmp_path: Path, provider: str
) -> None:
    variable = _variable(provider)
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
        state = catalog.resolve_at(fqid, 2002, variant="individuals")[0]
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


@pytest.mark.parametrize(
    "update",
    [
        {"valid_from": "2000"},
        {"valid_from": "20000101"},
        {"valid_from": "2000-02-30"},
        {"valid_from": "2001-01-01"},
        {"valid_to": "9999-12-31"},
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
        ResolvedVariant(slug="_default", name="Default")


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
    with pytest.raises(ValidationError, match="at least one finite state"):
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
