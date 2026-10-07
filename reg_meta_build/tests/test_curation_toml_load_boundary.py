"""Curation-TOML load and located-failure cases for the relations and doc-source maps.

- A curated same_as identity component may hold up to 32 FQIDs, the cap stated only in
  the comment on ``relations.py``'s private cap constant (the committed
  ``curation/relations.toml`` meets it at the real-seed build).
- The build refuses a curated same_as component above that cap (synthetic SCB
  register whose curated relations chain N variables).
- A malformed ``doc_sources.toml`` entry fails ``reg-meta-build build-docs`` with a
  located ``doc_sources_invalid`` configuration error (synthetic TOML written into
  a copied builder package, since the map resolves relative to the package file).
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import var_row
from _curation_toml_boundary_support import (
    SCB_SAMPLE_REGISTER,
    BuiltCatalog,
    build_scb_catalog,
    builder_copy,
    reset_doc_curation,
    run_build_docs,
    write_register_doc,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
    from pathlib import Path


def _same_as_chain_build(tmp_path: Path, size: int) -> BuiltCatalog:
    """Build one SCB register whose curated same_as edges chain ``size`` variables
    into a single identity component."""
    columns = [f"V{index}" for index in range(size)]
    return build_scb_catalog(
        tmp_path,
        curation={
            "registers/scb/sample.toml": SCB_SAMPLE_REGISTER
            + "".join(
                f'[[variable]]\nnative_id = "1.{100 + index}"\nslug = "v{index}"\n'
                for index in range(size)
            ),
            "relations.toml": "".join(
                f'[[edge]]\ntype = "same_as"\na = "scb/sample/v{index}"\n'
                f'b = "scb/sample/v{index + 1}"\n'
                for index in range(size - 1)
            ),
        },
        registerinformation_rows=[
            var_row(
                cvid=1000 + index, var_id=100 + index, colname=column, varname=column
            )
            for index, column in enumerate(columns)
        ],
        unika_rows=[
            f"TESTREG|Testregistret|Individer|Individer|{column}|{column}"
            "|2020|2020|0|0|0"
            for column in columns
        ],
    )


def test_same_as_component_at_the_32_fqid_cap_builds(tmp_path: Path) -> None:
    built = _same_as_chain_build(tmp_path, 32)
    assert built.result["status"] == "diagnostic_complete"
    with sqlite3.connect(built.db) as conn:
        # Both directions of each of the 31 chain edges are stored.
        assert conn.execute("SELECT COUNT(*) FROM variable_same_as").fetchone() == (62,)


def test_same_as_component_above_the_cap_fails_the_build(tmp_path: Path) -> None:
    with pytest.raises(RegMetaError) as exc_info:
        _same_as_chain_build(tmp_path, 33)
    assert exc_info.value.code == "relations_same_as_component_too_large"
    assert exc_info.value.exit_code == EXIT_CONFIG
    assert "component of 33 FQIDs (cap 32)" in exc_info.value.message


@pytest.fixture(scope="module")
def doc_builder(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return builder_copy(tmp_path_factory.mktemp("doc-sources"))


@pytest.mark.parametrize(
    "entry",
    ['title = "T"', 'url = ""\ntitle = "T"', 'url = 1\ntitle = "T"'],
    ids=["missing", "empty", "not-a-string"],
)
def test_malformed_doc_source_url_is_a_located_config_error(
    doc_builder: Path, tmp_path: Path, entry: str
) -> None:
    """A missing, empty or non-string ``url`` is an actionable configuration error
    naming the source slug, not a bare KeyError."""
    reset_doc_curation(doc_builder)
    (doc_builder / "doc_sources.toml").write_text(
        f'[sources."some-slug"]\n{entry}\n', encoding="utf-8"
    )
    write_register_doc(tmp_path / "docs")
    run = run_build_docs(doc_builder, tmp_path / "docs", tmp_path / "db")
    assert run.exit_code == EXIT_CONFIG
    assert run.payload["error"]["code"] == "doc_sources_invalid"
    assert "some-slug" in run.payload["error"]["message"]
    assert "`url`" in run.payload["error"]["message"]
