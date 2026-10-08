"""Curation-TOML load and located-failure cases for the relations, search-pin and
doc-source maps.

- A curated same_as identity component may hold up to 32 FQIDs, the cap stated only in
  the comment on ``relations.py``'s private cap constant (the committed
  ``curation/relations.toml`` meets it at the real-seed build).
- The build refuses a curated same_as component above that cap (synthetic SCB
  register whose curated relations chain N variables).
- The build refuses a duplicated, non-variable or self-referencing curated
  code<->label pair with an error naming its register file.
- Any build, diagnostic included, refuses a malformed ``search_pins.toml`` entry with
  a located ``search_pins_invalid`` error. An unresolved pin is a complete-build
  failure, pinned by ``conformance/cases/http_search/golden-unresolved``.
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


_PAIR = '[[code_label_pair]]\ncode = "scb/sample/{}"\nlabel = "scb/sample/{}"\n'


@pytest.mark.parametrize(
    ("pairs", "located"),
    [
        (_PAIR.format("kod", "namn") * 2, "[[code_label_pair]] entry 2: duplicate"),
        (_PAIR.format("kod", "people/namn"), "provider/register coordinate"),
        (_PAIR.format("kod", "kod"), "code_label_pair/1: identical endpoints"),
    ],
    ids=["duplicate", "wrong-grain", "self-pair"],
)
def test_malformed_code_label_pair_fails_the_build_located(
    tmp_path: Path, pairs: str, located: str
) -> None:
    """A duplicated, non-variable or self-referencing code<->label pair stops the
    build before resolution, naming the register file and the pair. The first two
    fail the register-file load (EXIT_CONFIG); a self-pair loads and fails the
    curation compile, which the CLI reports as EXIT_CONFIG."""
    with pytest.raises((RegMetaError, ValueError)) as exc_info:
        build_scb_catalog(
            tmp_path,
            curation={"registers/scb/sample.toml": SCB_SAMPLE_REGISTER + pairs},
            registerinformation_rows=[
                var_row(cvid=1000, var_id=100, colname="Kod", varname="Kod"),
                var_row(cvid=1001, var_id=101, colname="Namn", varname="Namn"),
            ],
        )
    error = exc_info.value
    message = error.message if isinstance(error, RegMetaError) else str(error)
    if isinstance(error, RegMetaError):
        assert error.exit_code == EXIT_CONFIG
    assert "registers/scb/sample.toml" in message
    assert located in message


_PIN = '[[pin]]\nquery = "{}"\ntype = "register"\nfqids = [{}]\n'


@pytest.mark.parametrize(
    ("pins", "located"),
    [
        (_PIN.format("q", '"scb/a", "scb/a"'), "entry 1: Value error, duplicate"),
        (_PIN.format("q", '"class/a"'), "entry 1: Value error, 'class/a' is a"),
        (
            _PIN.format("Sysselsättning", '"scb/a"')
            + _PIN.format(" sysselsattning", '"scb/b"'),
            "entry 2: duplicate register pin",
        ),
    ],
    ids=["duplicate-fqid", "wrong-type", "duplicate-folded-query"],
)
def test_malformed_search_pin_fails_the_build_located(
    tmp_path: Path, pins: str, located: str
) -> None:
    """A pin listing an fqid twice, an fqid of another type, or a query that folds
    like an earlier pin's fails even a diagnostic build, before anything is
    resolved. Fails if the pipeline stops loading the pins outside complete builds,
    or if `fold_search` no longer keys the duplicate check."""
    with pytest.raises(RegMetaError) as exc_info:
        build_scb_catalog(
            tmp_path,
            curation={"search_pins.toml": pins},
            registerinformation_rows=[
                var_row(cvid=1000, var_id=100, colname="Kod", varname="Kod")
            ],
        )
    assert exc_info.value.code == "search_pins_invalid"
    assert exc_info.value.exit_code == EXIT_CONFIG
    assert exc_info.value.message.startswith("search_pins.toml [[pin]] ")
    assert located in exc_info.value.message


def test_diagnostic_build_stores_no_search_pins(tmp_path: Path) -> None:
    """A diagnostic build stores no pins, so an unresolved one is not an error, and
    records the empty-pins hash (sha256 of `[]`). Fails if partial builds start
    writing or resolving pins."""
    built = build_scb_catalog(
        tmp_path,
        curation={
            "registers/scb/sample.toml": SCB_SAMPLE_REGISTER
            + '[[variable]]\nnative_id = "1.100"\nslug = "kod"\n',
            "search_pins.toml": _PIN.format("diagnos", '"sos/par"'),
        },
        registerinformation_rows=[
            var_row(cvid=1000, var_id=100, colname="Kod", varname="Kod")
        ],
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|Kod|Kod|2020|2020|0|0|0"
        ],
    )
    assert built.result["status"] == "diagnostic_complete"
    with sqlite3.connect(built.db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM search_pin").fetchone() == (0,)
        assert conn.execute(
            "SELECT value FROM import_manifest WHERE key = 'search_pins_sha256'"
        ).fetchone() == (
            "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
        )


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
