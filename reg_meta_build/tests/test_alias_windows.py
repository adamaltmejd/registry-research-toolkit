"""Register-scoped alias-window loader validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.alias_windows import load_alias_windows
from reg_meta_build.curation_compile import compile_alias_windows
from reg_meta_build.curation_tree import load_register_files
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_records import SourceRecord


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


def _alias_record(cvid: int) -> SourceRecord:
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
    return clean_scb_row(
        header,
        cvid,
        {
            name: (True, value, value)
            for name, value in zip(
                header,
                _var_row(colname="E_AWBUY", cvid=cvid, var_id=5, year="2018").split(
                    "|"
                ),
                strict=True,
            )
        },
        revision,
    ).record


def _alias_names(record: SourceRecord) -> tuple[NamingDeclaration, ...]:
    register_key = source_register_key(record)
    return tuple(
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


def test_alias_window_with_two_record_targets_is_overbroad(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry())
    (register,) = load_register_files(root)
    first, second = _alias_record(100), _alias_record(101)
    assert first.subject.native != second.subject.native
    cases, issues = compile_alias_windows(
        register, (first, second), _alias_names(first)
    )
    assert cases == ()
    assert [issue.code for issue in issues] == ["overbroad_curation_entry"]
    assert (
        "curation/registers/scb/testreg.toml#/representation.alias_window/1"
        in issues[0].detail
    )


def test_alias_window_deduplicates_identical_guard_subjects(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry())
    (register,) = load_register_files(root)
    record = _alias_record(100)
    cases, issues = compile_alias_windows(
        register, (record, record), _alias_names(record)
    )
    assert issues == ()
    assert len(cases) == 1
    assert len(cases[0].peer_guards) == 1


def test_alias_window_guards_same_ref_under_distinct_subjects(tmp_path: Path) -> None:
    root = _write_register(tmp_path, _entry())
    (register,) = load_register_files(root)
    record = _alias_record(100)
    other_native = record.subject.native.model_copy(update={"member_id": 101})
    other = record.model_copy(
        update={"subject": record.subject.model_copy(update={"native": other_native})}
    )
    cases, issues = compile_alias_windows(
        register, (record, other), _alias_names(record)
    )
    assert issues == ()
    assert len(cases) == 1
    assert len({guard.guard_id for guard in cases[0].peer_guards}) == 2


def test_alias_window_with_two_compiled_variable_keys_is_overbroad(
    tmp_path: Path,
) -> None:
    root = _write_register(tmp_path, _entry())
    (register,) = load_register_files(root)
    record = _alias_record(100)
    names = _alias_names(record)
    variable_key = native_variable_key(record)
    assert variable_key is not None
    second_key = (*variable_key, "split")
    second_variable = names[-1].model_copy(
        update={
            "target": names[-1].target.model_copy(update={"source_key": second_key})
        }
    )
    cases, issues = compile_alias_windows(
        register, (record,), (*names, second_variable)
    )
    assert cases == ()
    assert [issue.code for issue in issues] == ["overbroad_curation_entry"]
