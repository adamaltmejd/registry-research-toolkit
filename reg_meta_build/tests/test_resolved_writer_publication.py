"""Resolved writer preflight, model contracts and previous-catalog preservation."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import (
    resolved_state as _state,
    resolved_variable as _variable,
)
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta.errors import RegMetaError
from reg_meta_build.db import publish_db
from reg_meta_build.resolved_catalog import (
    CURATION_TREE_SHA256_KEY,
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedEdition,
    ResolvedState,
    ResolvedVariable,
    validate_resolved_variables,
    write_resolved_catalog,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_missing_common_name_still_requires_positive_delivery_names():
    with pytest.raises(ValidationError, match="positive delivery names"):
        ResolvedVariable.model_validate(
            _variable().model_copy(update={"name": None}).model_dump()
        )


@pytest.mark.parametrize("kind", ["register", "variant"])
def test_conflicting_independent_parents_fail_before_replacing_output(
    tmp_path: Path,
    kind: str,
) -> None:
    variable = _variable()
    output = tmp_path / "catalog.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    before = output.read_bytes()
    with pytest.raises(ValueError, match=f"inconsistent resolved parent {kind}"):
        write_resolved_catalog(
            (variable,),
            output,
            manifest=synthetic_manifest(),
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


def test_rerun_is_byte_identical_regardless_of_input_order(tmp_path: Path) -> None:
    variables = (_variable(), _variable("sos"))
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        variables, output, manifest=synthetic_manifest() | {"z": "last", "a": "first"}
    )
    original = output.read_bytes()
    reversed_variables = tuple(
        v.model_copy(update={"states": tuple(reversed(v.states))})
        for v in reversed(variables)
    )
    write_resolved_catalog(
        reversed_variables,
        output,
        manifest=synthetic_manifest() | {"a": "first", "z": "last"},
    )
    assert output.read_bytes() == original
    assert output.with_name("reg_meta.db.prev").read_bytes() == original


def test_published_artifact_carries_planner_statistics(tmp_path: Path) -> None:
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), output, manifest=synthetic_manifest())
    with closing(sqlite3.connect(output)) as conn:
        analyzed = set(conn.execute("SELECT tbl, idx FROM sqlite_stat1"))
    assert ("variable_state", "idx_variable_state_variable") in analyzed


@pytest.mark.parametrize("key", (CURATION_TREE_SHA256_KEY,))
@pytest.mark.parametrize("digest", ("short", "A" * 64))
def test_curation_manifest_hashes_require_lowercase_sha256(
    tmp_path: Path, key: str, digest: str
) -> None:
    output = tmp_path / "reg_meta.db"
    with pytest.raises(ValueError, match=f"manifest {key} must be a lowercase SHA-256"):
        write_resolved_catalog(
            (_variable(),), output, manifest=synthetic_manifest() | {key: digest}
        )
    assert not output.exists()


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
        write_resolved_catalog(
            (variable,), output, manifest=synthetic_manifest(), editions=editions
        )
    assert output.read_bytes() == b"previous"


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
            manifest=synthetic_manifest(),
        )
    assert not output.exists()


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
        write_resolved_catalog(
            (variable,), output, manifest=synthetic_manifest(), diagnostic=diagnostic
        )
    assert not output.exists()


def test_diagnostic_artifact_is_create_only_and_builder_cannot_publish_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    normal = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), normal, manifest=synthetic_manifest())
    original = normal.read_bytes()
    diagnostic = tmp_path / "diagnostic.db"
    write_resolved_catalog(
        (_variable(),), diagnostic, manifest=synthetic_manifest(), diagnostic=True
    )
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
                (_variable(),),
                destination,
                manifest=synthetic_manifest(),
                diagnostic=True,
            )
    active = tmp_path / "absent-active"
    monkeypatch.setenv("REG_META_DB", str(active))
    with pytest.raises(ValueError, match="new explicit path"):
        write_resolved_catalog(
            (_variable(),),
            active / "reg_meta.db",
            manifest=synthetic_manifest(),
            diagnostic=True,
        )
    assert not active.exists()


@pytest.mark.parametrize("field", ["is_sensitive", "is_identifier"])
@pytest.mark.parametrize("value", [0, 1, "false", "true"])
def test_malformed_flags_fail_shared_preflight_before_publication(
    tmp_path: Path, field: str, value: int | str
) -> None:
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), output, manifest=synthetic_manifest())
    original = output.read_bytes()
    variable = _variable().model_copy(update={field: value})
    with pytest.raises(ValidationError, match=field):
        validate_resolved_variables((variable,))
    with pytest.raises(ValidationError, match=field):
        write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
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
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
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
