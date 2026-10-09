"""A malformed ``doc_sources.toml`` entry fails ``reg-meta-build build-docs`` with a
located ``doc_sources_invalid`` configuration error.

The map resolves relative to the package file, so the cases write synthetic TOML into a
copied builder package. The build-located curation-TOML refusals (same_as cap,
code<->label pairs, search pins) are build cases under `cases/build/`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _curation_toml_boundary_support import (
    builder_copy,
    reset_doc_curation,
    run_build_docs,
    write_register_doc,
)
from reg_meta_build.errors import EXIT_CONFIG

if TYPE_CHECKING:
    from pathlib import Path


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
