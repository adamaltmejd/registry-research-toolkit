"""prepare-sources reads the authored sources from the curation tree, not the bundle."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import write_scb_input
from _input_repo_fixtures import write_input_bundle
from reg_meta_build.cli import run
from reg_meta_build.errors import EXIT_CONFIG
from reg_meta_build.prepared_catalog import (
    PreparedCatalogError,
    prepare_catalog_sources,
)

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.input_snapshot import CatalogBundleSelection


def _selection(tmp_path: Path) -> CatalogBundleSelection:
    source = tmp_path / "source"
    write_scb_input(source)
    return write_input_bundle(tmp_path / "accepted", source)


def test_preparation_reads_authored_sources_from_the_curation_tree(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Fails if prepare-sources stops reading the authored TOMLs from
    `--curation-dir` (an invalid one then prepares), or re-codes the located
    curated-source refusal as its generic source_preparation_failed."""
    selection = _selection(tmp_path)
    providers = tmp_path / "curation" / "providers"
    providers.mkdir(parents=True)
    (providers / "fohm.toml").write_text("not = [valid", encoding="utf-8")
    destination = tmp_path / "prepared" / "catalog"
    argv = [
        "prepare-sources",
        "--input-bundle",
        str(selection.path),
        "--input-commit",
        selection.input_commit,
        "--input-manifest-sha256",
        selection.manifest_sha256,
        "--output-dir",
        str(destination),
        "--curation-dir",
        str(tmp_path / "curation"),
    ]
    assert run(argv) == EXIT_CONFIG
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "curated_source_invalid"
    assert not destination.exists()


def test_preparation_refuses_a_canonical_list_the_bundle_lacks(tmp_path: Path) -> None:
    """Fails if prepare accepts a `scb_canonical.toml` whose declared code list the
    bundle never captured, preparing a canonical register whose list cannot bind."""
    selection = _selection(tmp_path)
    authored = tmp_path / "curation" / "scb_canonical"
    authored.mkdir(parents=True)
    (authored / "scb_canonical.toml").write_text(
        '[[register]]\nkey = "example"\nname = "Example"\n'
        '[[register.variable]]\nname = "Code"\ncolumn = "Code"\n'
        'value_set = "undelivered"\n',
        encoding="utf-8",
    )
    destination = tmp_path / "prepared" / "catalog"
    with pytest.raises(PreparedCatalogError, match="undelivered.csv"):
        prepare_catalog_sources(
            selection, destination, curation_dir=tmp_path / "curation"
        )
    assert not destination.exists()
