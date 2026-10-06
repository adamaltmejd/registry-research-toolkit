"""Exercise build-db from accepted prepared sources and tracked curation."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from _pipeline_catalog_support import (
        CatalogFixture,
    )


def test_source_cannot_mix_whole_and_register_scopes(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build.prepared_sources import PreparedSourceRecords

    original = PreparedSourceRecords.register_coordinates

    def mixed(self, source):
        coordinates = original(self, source)
        if source == "scb-registerinformation":
            return (*coordinates, (None, coordinates[0][1], 1))
        return coordinates

    monkeypatch.setattr(PreparedSourceRecords, "register_coordinates", mixed)
    with pytest.raises(ValueError, match="both whole-source and register scopes"):
        catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


@pytest.mark.parametrize("local", [False, True])
def test_source_occurrence_accounting_cannot_drop_a_record(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, local: bool
) -> None:
    from reg_meta_build import pipeline

    resolve = pipeline.resolve_source_scope

    def drop_occurrence(*args, **kwargs):
        result = resolve(*args, **kwargs)
        return replace(result, corrections=replace(result.corrections, occurrences=()))

    monkeypatch.setattr(pipeline, "resolve_source_scope", drop_occurrence)
    with pytest.raises(ValueError, match="physical source occurrence accounting"):
        if local:
            catalog.check(tmp_path / "report")
        else:
            catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


def test_compiled_global_contract_rejects_unknown_field(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch
) -> None:
    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation

    def invalid(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        return replace(compiled, fields={**compiled.fields, "unknown": True})

    monkeypatch.setattr(pipeline, "compile_curation", invalid)
    with pytest.raises(ValueError, match="unknown"):
        catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


@pytest.mark.parametrize("local", [False, True])
def test_compiled_global_contract_revalidates_serialized_nested_field(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, local: bool
) -> None:
    from reg_meta_build.curation_compile import CompiledCodebook

    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation

    def invalid(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        book = CompiledCodebook("invalid", "invalid", {"broken": ["invalid"]})  # ty: ignore[invalid-argument-type]
        return replace(compiled, fields={**compiled.fields, "classifications": (book,)})

    monkeypatch.setattr(pipeline, "compile_curation", invalid)
    with pytest.raises(ValueError, match="classifications.0.metadata.broken"):
        if local:
            catalog.check(tmp_path / "report")
        else:
            catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


@pytest.mark.parametrize("local", [False, True])
def test_compiled_scope_contract_revalidates_serialized_naming(
    catalog: CatalogFixture, tmp_path: Path, monkeypatch, local: bool
) -> None:
    from reg_meta_build import pipeline

    compile_tree = pipeline.compile_curation

    def invalid(*args, **kwargs):
        compiled = compile_tree(*args, **kwargs)
        key, declarations = next(iter((compiled.naming or {}).items()))
        declaration = declarations[0]
        target = declaration.target.model_copy(update={"source_key": ()})
        naming = declaration.model_copy(update={"target": target})
        return replace(
            compiled,
            naming={**(compiled.naming or {}), key: (naming, *declarations[1:])},
        )

    monkeypatch.setattr(pipeline, "compile_curation", invalid)
    with pytest.raises(ValueError, match="nonempty exact source key"):
        if local:
            catalog.check(tmp_path / "report")
        else:
            catalog.build(tmp_path / "bad.db", tmp_path / "report", registers=("1",))
    assert not (tmp_path / "bad.db").exists()


@pytest.mark.parametrize(
    "field",
    [
        "cases",
        "source_diagnostics",
        "naming",
        "naming_ambiguities",
        "provider_keys",
        "variants",
    ],
)
@pytest.mark.parametrize("invalid", [{}, None, "not an array"])
def test_compiled_scope_json_rejects_invalid_collection_containers(
    field: str, invalid: object
) -> None:
    from pydantic import ValidationError
    from reg_meta_build.pipeline import _validate_compiled_scope

    with pytest.raises(ValidationError) as caught:
        _validate_compiled_scope(
            {"source": "fixture", "register_key": None, field: invalid}
        )
    assert caught.value.errors()[0]["loc"] == (field,)


def test_compiled_scope_json_preserves_order_types_and_failure_coordinates() -> None:
    from pydantic import ConfigDict, TypeAdapter, ValidationError
    from reg_meta_build.pipeline import CompiledScope, _validate_compiled_scope

    serializer = TypeAdapter(object, config=ConfigDict(ser_json_inf_nan="constants"))
    provider_keys = ((("member", 1), "Å"), (("member", "1"), None))
    data = {
        "source": "källa",
        "register_key": ("register", 1),
        "provider_keys": provider_keys,
    }
    baseline = CompiledScope.model_validate_json(serializer.dump_json(data))
    parsed = _validate_compiled_scope(data)
    assert parsed.model_dump_json() == baseline.model_dump_json()
    invalid = {**data, "provider_keys": (*provider_keys, ((True,), "B"))}
    with pytest.raises(ValidationError) as old:
        CompiledScope.model_validate_json(serializer.dump_json(invalid))
    with pytest.raises(ValidationError) as new:
        _validate_compiled_scope(invalid)
    for caught in (old, new):
        assert caught.value.errors()[0]["loc"][:2] == ("provider_keys", 2)
    assert [(e["loc"], e["type"]) for e in old.value.errors()] == [
        (e["loc"], e["type"]) for e in new.value.errors()
    ]
    for replacement in ({"source": 1}, {"unknown": True}):
        with pytest.raises(ValidationError):
            _validate_compiled_scope({**data, **replacement})
