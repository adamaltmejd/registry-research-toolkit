"""A catalog whose structural validation fails is never placed.

The failures are genuine `validate_built_db` refusals of the staged artifact: a
variant panel key that names no variable in its register (the build pipeline
refuses that curation earlier, so the writer is called directly), and a small
catalog written against the real-corpus volume floors.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import var_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from catalog_manifest import synthetic_manifest
from reg_meta_build.pipeline import build_catalog
from reg_meta_build.prepared_catalog import prepare_catalog_sources
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
    write_resolved_catalog,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

VALIDATION_FAILED = "resolved catalog validation failed"


@pytest.fixture
def sample_build(tmp_path: Path) -> Callable[..., dict[str, object]]:
    """One accepted SCB register with one curated variable."""
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=[
            var_row(cvid=1001, var_id=101, colname="VALUE", data_type="int")
        ],
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
        ],
        include=("registerinformation", "unika"),
    )
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    commit = accept_prepared(prepared)
    curation = tmp_path / "curation"
    registers = curation / "registers" / "scb"
    registers.mkdir(parents=True)
    (curation / "classifications").mkdir()
    (registers / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
        '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
        encoding="utf-8",
    )

    def build(output: Path, report: Path, **kwargs) -> dict[str, object]:
        return build_catalog(
            prepared,
            commit,
            manifest.sha256,
            output,
            report,
            curation_dir=curation,
            **kwargs,
        )

    return build


def _variable(*, panel_entity_key: str | None = None) -> ResolvedVariable:
    return ResolvedVariable(
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        slug="value",
        provider_key="101",
        name="Value",
        definition=None,
        description=None,
        operational_definition=None,
        measurement_unit=None,
        is_sensitive=False,
        is_identifier=False,
        states=(
            ResolvedState(
                variant=ResolvedVariant(
                    slug="people", name="People", panel_entity_key=panel_entity_key
                ),
                valid_from="2020-01-01",
                valid_to="2020-12-31",
                delivery_column_name="VALUE",
                data_type="int",
                data_length=None,
                operational_definition=None,
                provenance=None,
            ),
        ),
    )


def test_diagnostic_failing_validation_is_never_created(tmp_path: Path) -> None:
    output = tmp_path / "diagnostic.db"

    with pytest.raises(ValueError, match=f"{VALIDATION_FAILED}.*panel_entity_key"):
        write_resolved_catalog(
            (_variable(panel_entity_key="missing"),),
            output,
            manifest=synthetic_manifest(),
            diagnostic=True,
        )

    assert sorted(path.name for path in tmp_path.iterdir()) == []


def test_catalog_failing_validation_keeps_the_previous_catalog(
    fixture_db: Path, tmp_path: Path
) -> None:
    active = tmp_path / "active"
    active.mkdir()
    output = active / "reg_meta.db"
    previous = fixture_db.read_bytes()
    output.write_bytes(previous)

    with pytest.raises(ValueError, match=f"{VALIDATION_FAILED}.*panel_entity_key"):
        write_resolved_catalog(
            (_variable(panel_entity_key="missing"),),
            output,
            manifest=synthetic_manifest(),
        )

    assert output.read_bytes() == previous
    assert sorted(path.name for path in active.iterdir()) == ["reg_meta.db"]


def test_corpus_write_is_validated_against_the_corpus_floors(tmp_path: Path) -> None:
    output = tmp_path / "diagnostic.db"
    write_resolved_catalog(
        (_variable(),), tmp_path / "small.db", manifest=synthetic_manifest()
    )

    # The same small catalog that passes structural validation cannot meet the
    # real-corpus volume floors.
    with pytest.raises(ValueError, match=f"{VALIDATION_FAILED}.*corpus build"):
        write_resolved_catalog(
            (_variable(),),
            output,
            manifest=synthetic_manifest(),
            diagnostic=True,
            corpus=True,
        )

    assert not output.exists()


def test_diagnostic_never_replaces_a_destination_created_during_the_build(
    sample_build: Callable[..., dict[str, object]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    output = out_dir / "diagnostic.db"
    real_link = os.link

    # Filesystem boundary: another process creates the destination just before
    # the finished diagnostic is placed.
    def competing_link(src, dst, *args, **kwargs):
        if os.fspath(dst) == os.fspath(output):
            output.write_bytes(b"published concurrently")
        return real_link(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, "link", competing_link)

    with pytest.raises(FileExistsError):
        sample_build(output, tmp_path / "report", diagnostic=True)

    assert output.read_bytes() == b"published concurrently"
    assert sorted(path.name for path in out_dir.iterdir()) == ["diagnostic.db"]
