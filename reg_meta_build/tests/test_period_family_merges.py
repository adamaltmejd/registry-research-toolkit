"""Register-scoped representation-period loader validation."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.curation_compile import compile_matrix_repr, compile_period_families
from reg_meta_build.curation_tree import load_register_files
from reg_meta_build.period_family_merges import PeriodFamily, load_period_family_merges
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_coordinates import column_identity, source_register_key
from reg_meta_build.source_curation import RepresentationDecision
from reg_meta_build.source_effects import apply_occurrence_cases
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import SourceRevision, TemporalScope
from reg_meta_build.source_representations import resolve_representation_cases
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path


def _write_register(tmp_path: Path, body: str) -> Path:
    root = tmp_path / "curation"
    path = root / "registers" / "scb" / "lisa.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "34"\n\n' + body,
        encoding="utf-8",
    )
    return root


def test_load_period_family_parses_explicit_slug(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\n'
        'slug = "lone-eller-foretagarinkomst-manad"\n',
    )
    assert load_period_family_merges(root) == (
        PeriodFamily(
            "scb",
            "lisa",
            "lonfink",
            "Lön per månad",
            "lone-eller-foretagarinkomst-manad",
        ),
    )


def test_load_period_family_requires_authored_slug(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "curation/registers/scb/lisa.toml" in exc.value.message
    assert "[[representation.period_family.slug]] entry 1" in exc.value.message
    assert "slug" in exc.value.message


def test_load_period_family_rejects_unknown_key(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\nslug = "lonfink"\nunknown = "x"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "entry 1" in exc.value.message


def test_load_period_family_rejects_wrong_register(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/rams"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\nslug = "lonfink"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(root)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "does not match" in exc.value.message


def test_load_period_family_rejects_duplicate_stem(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "x"\nlabel = "A"\nslug = "x"\n\n'
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "x"\nlabel = "B"\nslug = "x"\n',
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(root)
    assert exc.value.code == "period_family_merges_invalid"


def test_load_period_family_empty_when_no_tree() -> None:
    assert load_period_family_merges(None) == ()


def _month_records(count: int):
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
    months = (
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "Maj",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Okt",
        "Nov",
        "Dec",
    )
    return tuple(
        clean_scb_row(
            header,
            index,
            {
                name: (True, value, value)
                for name, value in zip(
                    header,
                    _var_row(
                        colname=f"LonFink{month}",
                        cvid=100 + index,
                        var_id=index,
                        year="2020",
                        register=("LISA", 34, 10),
                    ).split("|"),
                    strict=True,
                )
            },
            revision,
        ).record
        for index, month in enumerate(months[:count], 1)
    )


def test_compile_twelve_months_and_missing_month(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\n'
        'slug = "lone-per-manad"\n',
    )
    (register,) = load_register_files(root)
    records = _month_records(12)
    cases, names, keys, issues = compile_period_families(register, records)
    assert issues == ()
    assert len(cases) == 2
    assert names[0].naming.slug == "lone-per-manad"
    assert keys[0][1] == "period-family:lisa:lonfink"
    decision = next(
        case.decision
        for case in cases
        if isinstance(case.decision, RepresentationDecision)
    )
    assert len(decision.columns) == 12
    assert decision.columns[1].valid_to == "2020-02-29"
    result = apply_occurrence_cases(records, (cases[0],))
    assert result.diagnostics == ()
    assert {item.variable_key for item in result.occurrences} == {decision.variable_key}
    coding = {
        column_identity(
            decision.variable_key, decision.variant_key, column.column
        ): resolve_code_membership(())
        for column in decision.columns
    }
    resolution = resolve_representation_cases(records, (cases[1],), coding=coding)
    assert resolution.diagnostics == ()
    cases, names, keys, issues = compile_period_families(register, _month_records(11))
    assert cases == names == keys == ()
    assert [issue.code for issue in issues] == ["stale_curation_entry"]


def test_month_without_assignable_year_is_stale(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\nslug = "lonfink"\n',
    )
    (register,) = load_register_files(root)
    records = _month_records(12)
    undated = records[0].model_copy(
        update={
            "edition_scope": TemporalScope(kind="unknown", label="year unavailable")
        }
    )
    cases, names, keys, issues = compile_period_families(
        register, (undated, *records[1:])
    )
    assert cases == names == keys == ()
    assert [issue.code for issue in issues] == ["stale_curation_entry"]
    assert (
        "curation/registers/scb/lisa.toml#/representation.period_family/1"
        in issues[0].detail
    )
    assert "LonFinkJan" in issues[0].detail


def test_matrix_repr_wires_period_cases_into_selected_scope(tmp_path: Path) -> None:
    root = _write_register(
        tmp_path,
        '[[representation.period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön per månad"\nslug = "lonfink"\n',
    )
    (register,) = load_register_files(root)
    records = _month_records(12)
    register_key = source_register_key(records[0])
    named_register = NamingDeclaration(
        target=NativeNamingTarget(
            kind="register", provider="scb", source_key=register_key
        ),
        naming=SlugEntry(kind="register", provider="scb", source_id="34", slug="lisa"),
        contributors=(),
    )
    scope = CompiledScope(
        source=records[0].source,
        register_key=register_key,
        naming=(named_register,),
    )

    class Records:
        def iter_register_slices(self, source, wanted):
            assert source == scope.source and register_key in wanted
            yield register_key, records

    tree = SimpleNamespace(root=root, registers=(register,))
    prepared = SimpleNamespace(records=Records(), value_sources=())
    cases, names, keys, issues = compile_matrix_repr(
        tree, prepared, (scope,), {(scope.source, register_key): (named_register,)}
    )
    key = scope.source, register_key
    assert issues == ()
    assert len(cases[key]) == 2
    assert len(names[key]) == len(keys[key]) == 1
