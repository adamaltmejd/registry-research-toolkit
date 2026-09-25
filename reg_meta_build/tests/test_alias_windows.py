"""Register-scoped alias-window loader validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.alias_windows import load_alias_windows
from reg_meta_build.curation_compile import compile_alias_windows
from reg_meta_build.curation_tree import load_register_files
from reg_meta_build.fqid_slugs import SlugEntry
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import SourceRevision
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path


def _write_register(tmp_path: Path, entry: str) -> Path:
    root = tmp_path / "curation"
    path = root / "registers" / "scb" / "testreg.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "testreg"\nnative_id = "1"\n\n' + entry,
        encoding="utf-8",
    )
    return root


def _entry(variable: str = "scb/testreg/test-variable", extra: str = "") -> str:
    return (
        "[[representation.alias_window]]\n"
        f'variable = "{variable}"\n'
        'variant = "test-variant"\ncolumn = "AEBUY"\n'
        'source_editions = ["2018"]\nevidence = "held"\n'
        'noted = "2026-09-13"\n' + extra
    )


def test_valid_alias_window_parses(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry())
    (alias,) = load_alias_windows(root)
    assert (alias.fqid, alias.variant, alias.column, alias.source_editions) == (
        "scb/testreg/test-variable",
        "test-variant",
        "AEBUY",
        ("2018",),
    )


def test_unknown_key_is_rejected(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry(extra='witness = "2015"\n'))
    with pytest.raises(RegMetaError) as exc:
        load_alias_windows(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "entry 1" in exc.value.message


def test_wrong_register_is_rejected(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry("scb/other/test-variable"))
    with pytest.raises(RegMetaError) as exc:
        load_alias_windows(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "does not match" in exc.value.message


def test_invalid_noted_date_is_rejected(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry().replace("2026-09-13", "soon"))
    with pytest.raises(RegMetaError) as exc:
        load_alias_windows(root)
    assert "YYYY-MM-DD" in exc.value.message


def test_unresolved_compiled_variable_fqid_is_stale(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry())
    (register,) = load_register_files(root)
    cases, issues = compile_alias_windows(register, (), ())
    assert cases == ()
    assert [issue.code for issue in issues] == ["stale_curation_entry"]
    assert "alias_window/1" in issues[0].case_id


def test_alias_window_uses_named_keys_and_source_edition(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry())
    (register,) = load_register_files(root)
    revision = SourceRevision.create(
        dataset="scb-registerinformation",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="rows.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    record = clean_scb_row(
        header,
        1,
        {
            name: (True, value, value)
            for name, value in zip(
                header,
                _var_row(colname="E_AWBUY", cvid=100, var_id=5, year="2018").split("|"),
                strict=True,
            )
        },
        revision,
    ).record
    register_key = source_register_key(record)
    declarations = tuple(
        NamingDeclaration(
            target=NativeNamingTarget(
                kind=kind,
                provider="scb",
                source_key=key,
                register_key=register_key if kind != "register" else None,
            ),
            naming=SlugEntry(kind=kind, provider="scb", source_id=source_id, slug=slug),
            contributors=(),
        )
        for kind, key, source_id, slug in (
            ("register", register_key, "1", "testreg"),
            ("register_variant", native_variant_key(record), "1.10", "test-variant"),
            ("variable", native_variable_key(record), "1.5", "test-variable"),
        )
    )
    cases, issues = compile_alias_windows(register, (record,), declarations)
    assert issues == ()
    assert len(cases) == 1
    assert cases[0].decision.valid_from == "2018-01-01"
    assert cases[0].decision.valid_to == "2018-12-31"
    assert cases[0].decision.variable_key == native_variable_key(record)
