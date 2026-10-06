"""Checked `[[errata.support]]` source-support decisions at the build boundary.

Every case prepares synthetic SCB Registerinformation and Vardemangder rows
(`_csv_fixtures.var_row`; register TESTREG = native register 1, variant 1.10) and a
curation TOML through the real pipeline. The support entry is authored from the
prepared records the way a curator reads them (public `capture_expectations` and
record dumps). Assertions read each source occurrence's ledger disposition, the
issue ledger, the built catalog and curation-TOML located load failures.

`expected_coding_sha256` has no public authoring tool: the two digests below were
recorded by running the code under test on these exact fixtures, and they cover
only record references, literal columns and copied coding fingerprints.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import var_row
from _curation_support_boundary_support import (
    SCB_SAMPLE,
    SCB_VARIANT,
    prepare_sources,
    toml_inline,
)
from reg_meta.errors import RegMetaError
from reg_meta_build.source_curation import capture_expectations
from reg_meta_build.source_records import SourceFields

if TYPE_CHECKING:
    from pathlib import Path

# Recorded by running the code under test on the fixtures below (see docstring).
QUESTION_CODING_SHA256 = (
    "96c8c7e3a00a4d463682506ea758b8b5341cc0bcacae2f85e23b2243c1b72896"
)
NONPHYSICAL_CODING_SHA256 = (
    "71aaef38bb90625035a38986013a58560ae3b7378fdfe62a40f1f573a26c3259"
)
SUPPORT = "curation/registers/scb/sample.toml#/errata.support/1"
PROSE = ("name", "definition", "description", "operational_definition")
EVIDENCE = "Primary questionnaire identifies the authoritative physical question"


def _row(column: str, cvid: int, year: str, variable: int = 5, **fields) -> str:
    if variable == 6:
        fields.setdefault("varname", "Authority")
    return var_row(
        cvid=cvid,
        var_id=variable,
        colname=column,
        year=year,
        regver_id=int(year),
        **fields,
    )


def _owner(column: str, slug: str) -> str:
    return (
        f'[[variable]]\nnative_id = "1.5.{slug}"\nslug = "{slug}"\n'
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.10"\n'
        f'column = "{column}"\nowner = "1.5.{slug}"\nref = "{slug} construct"\n'
        'source_editions = ["2020"]\n'
    )


# Variable 1.5 delivers FIRST and SECOND under one source record (CVID 20); the
# FIRST projection asserts a question that variable 1.6 supplies authoritatively.
QUESTION_ROWS = {
    "target": _row("FIRST", 20, "2020"),
    "twin": _row("SECOND", 20, "2020"),
    "authority": _row("FIRST", 21, "2020", 6),
    "authority-later": _row("FIRST", 22, "2021", 6),
}
QUESTION_VALUES = {
    "target": "Svar|1|1|Legacy category|20|7102",
    "authority": "Svar|1|1|Authoritative yes|21|7101",
    "authority-later": "Svar|1|1|Authoritative yes|22|7101",
}
QUESTION_CURATION = (
    SCB_SAMPLE
    + SCB_VARIANT
    + _owner("FIRST", "first")
    + _owner("SECOND", "second")
    + '[[variable]]\nnative_id = "1.6"\nslug = "authority"\n'
)


def _selector(record, fields=PROSE, evidence=EVIDENCE) -> dict:
    """The curator's selector of one prepared record and its guarded originals."""
    expected = capture_expectations((record,), fields=fields)[0].alternatives[0]
    return {
        "variable": f"1.{record.subject.native.variable_id}",
        "variant": "1.10",
        "column": (record.fields.column_name and record.fields.column_name.value) or "",
        "edition": str(record.subject.native.edition_id),
        "expected_fields": [f.model_dump(mode="json") for f in expected.fields],
        "expected_period_text": record.original_period_text,
        "expected_scope": record.edition_scope.model_dump(mode="json"),
        "expected_period": record.edition_period_scope.model_dump(mode="json"),
        "evidence": evidence,
        "noted": "2026-09-30",
    }


def _support_entry(entry: dict) -> str:
    return "[[errata.support]]\n" + "".join(
        f"{key} = {toml_inline(value)}\n" for key, value in entry.items()
    )


def _record(records, variable: int, column: str, year: int):
    return next(
        r
        for r in records
        if r.subject.native.variable_id == variable
        and ((r.fields.column_name and r.fields.column_name.value) or "") == column
        and r.subject.native.edition_id == year
    )


def _question_entry(tmp_path: Path) -> dict:
    """The support entry authored from the unchanged question fixture."""
    sources = prepare_sources(
        tmp_path / "authored",
        scb_rows=list(QUESTION_ROWS.values()),
        vardemangder_rows=list(QUESTION_VALUES.values()),
        curation={"scb/sample.toml": QUESTION_CURATION},
    )
    records = sources.records()
    return {
        **_selector(_record(records, 5, "FIRST", 2020)),
        "authority": _selector(_record(records, 6, "FIRST", 2020)),
        "expected_coding_sha256": QUESTION_CODING_SHA256,
    }


def _question_sources(tmp_path: Path, entry: dict, rows=None, values=None):
    rows = {**QUESTION_ROWS, **(rows or {})}
    values = {**QUESTION_VALUES, **(values or {})}
    return prepare_sources(
        tmp_path,
        scb_rows=[row for row in rows.values() if row is not None],
        vardemangder_rows=[row for row in values.values() if row is not None],
        curation={"scb/sample.toml": QUESTION_CURATION + _support_entry(entry)},
    )


def _stale(built) -> list[str]:
    return [
        issue["detail"].split(":", 1)[0]
        if issue["detail"].startswith("curation/")
        else issue["case_id"]
        for issue in built.issues("stale_curation_entry")
    ]


def test_checked_support_keeps_the_twin_and_complete_authority_in_catalog(
    tmp_path: Path,
):
    """Only the contradicted projection becomes support; its twin and authority stay."""
    sources = _question_sources(tmp_path, _question_entry(tmp_path))
    built = sources.build(tmp_path)
    assert not built.issues()
    assert built.uses(sources.records()) == [
        ("FIRST", "member:20", "support", None),
        ("FIRST", "member:21", "catalog", "scb/sample/authority"),
        ("FIRST", "member:22", "catalog", "scb/sample/authority"),
        ("SECOND", "member:20", "catalog", "scb/sample/second"),
    ]
    assert built.columns("sample") == {"authority": {"FIRST"}, "second": {"SECOND"}}


@pytest.mark.parametrize(
    ("rows", "values"),
    [
        pytest.param(
            {"authority-later": None}, {"authority-later": None}, id="missing"
        ),
        pytest.param(
            {"authority-extra": _row("FIRST", 23, "2022", 6)},
            {"authority-extra": "Svar|1|1|Authoritative yes|23|7101"},
            id="extra",
        ),
        pytest.param(
            {"authority": _row("FIRST", 21, "2020", 6, varname="Changed")},
            {},
            id="prose",
        ),
        pytest.param(
            {"authority": _row("CHANGED", 21, "2020", 6)},
            {},
            id="literal",
        ),
        pytest.param(
            {},
            {"authority-later": "Ny version|1|1|Authoritative yes|22|7103"},
            id="coding-reference",
        ),
    ],
)
def test_checked_support_is_stale_when_its_authority_changes(
    tmp_path: Path, rows, values
):
    """The support decision guards the complete authoritative family and its coding."""
    sources = _question_sources(tmp_path, _question_entry(tmp_path), rows, values)
    built = sources.build(tmp_path)
    assert SUPPORT in _stale(built)
    assert "support" not in {use[2] for use in built.uses(sources.records())}


def test_checked_support_is_stale_when_authority_codes_change(tmp_path: Path):
    """A changed external code label of the authority invalidates the decision."""
    sources = _question_sources(
        tmp_path,
        _question_entry(tmp_path),
        values={
            "authority": "Svar|1|1|Changed source category|21|7101",
            "authority-later": "Svar|1|1|Changed source category|22|7101",
        },
    )
    built = sources.build(tmp_path)
    assert SUPPORT in _stale(built)
    assert "support" not in {use[2] for use in built.uses(sources.records())}


@pytest.mark.parametrize(
    ("coordinate", "value"),
    [
        ("variable", "2.6"),
        ("variant", "1.3"),
        ("column", "SECOND"),
        ("edition", "2021"),
    ],
)
def test_checked_support_authority_must_ask_the_same_physical_question(
    tmp_path: Path, coordinate: str, value: str
):
    """An authority in another register, variant, column or edition is refused at load."""
    entry = _question_entry(tmp_path)
    entry["authority"][coordinate] = value
    sources = _question_sources(tmp_path, entry)
    with pytest.raises(RegMetaError) as raised:
        sources.build(tmp_path)
    assert raised.value.code == "classification_curation_invalid"
    assert raised.value.message.startswith("curation/registers/scb/sample.toml")
    assert "same source-local physical question" in raised.value.message


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param({"target": None}, id="missing"),
        pytest.param({"third": _row("THIRD", 20, "2020")}, id="extra"),
        pytest.param(
            {"target": _row("FIRST", 20, "2020", vardef="Changed")}, id="prose"
        ),
    ],
)
def test_checked_support_is_stale_when_its_shared_target_changes(tmp_path: Path, rows):
    """The decision guards every projection sharing the contradicted source record."""
    sources = _question_sources(tmp_path, _question_entry(tmp_path), rows)
    built = sources.build(tmp_path)
    assert SUPPORT in _stale(built)
    assert "support" not in {use[2] for use in built.uses(sources.records())}


# Variable 1.5 delivers a literal blank column (quoted empty Kolumnnamn, SCB's
# "no physical column") at 2021 (CVID 21); its FIRST
# delivery at 2020 (CVID 20) witnesses the same quantity.
NONPHYSICAL_ROWS = {
    "target": _row('""', 21, "2021"),
    "authority": _row("FIRST", 20, "2020"),
}
NONPHYSICAL_CURATION = (
    SCB_SAMPLE + SCB_VARIANT + '[[variable]]\nnative_id = "1.5"\nslug = "quantity"\n'
)
NONPHYSICAL_EVIDENCE = (
    "A blank physical column is source support; the witness establishes quantity only."
)


def _nonphysical_entry(tmp_path: Path) -> dict:
    sources = prepare_sources(
        tmp_path / "authored",
        scb_rows=list(NONPHYSICAL_ROWS.values()),
        curation={"scb/sample.toml": NONPHYSICAL_CURATION},
    )
    records = sources.records()
    return {
        **_selector(
            _record(records, 5, "", 2021),
            tuple(SourceFields.model_fields),
            NONPHYSICAL_EVIDENCE,
        ),
        "kind": "nonphysical_projection",
        "authority": _selector(
            _record(records, 5, "FIRST", 2020), evidence=NONPHYSICAL_EVIDENCE
        ),
        "expected_coding_sha256": NONPHYSICAL_CODING_SHA256,
    }


def _nonphysical_sources(tmp_path: Path, entry: dict, rows=None):
    rows = {**NONPHYSICAL_ROWS, **(rows or {})}
    return prepare_sources(
        tmp_path,
        scb_rows=[row for row in rows.values() if row is not None],
        curation={"scb/sample.toml": NONPHYSICAL_CURATION + _support_entry(entry)},
    )


def test_nonphysical_support_keeps_its_quantity_witness_in_catalog(tmp_path: Path):
    """A blank-column projection becomes support; its witness stays catalog."""
    sources = _nonphysical_sources(tmp_path, _nonphysical_entry(tmp_path))
    built = sources.build(tmp_path)
    assert not built.issues()
    assert built.uses(sources.records()) == [
        ("FIRST", "member:20", "catalog", "scb/sample/quantity"),
        (None, "member:21", "support", "scb/sample/quantity"),
    ]
    assert built.columns("sample") == {"quantity": {"FIRST"}}


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param({"target": None}, id="witness-only"),
        pytest.param({"authority": None}, id="projection-only"),
        pytest.param({"extra": _row("SECOND", 22, "2022")}, id="extra-original"),
        pytest.param({"target": _row("NEW", 21, "2021")}, id="column-now-physical"),
    ],
)
def test_nonphysical_support_is_stale_when_its_family_changes(tmp_path: Path, rows):
    """The projection and its witness are a complete guarded family."""
    sources = _nonphysical_sources(tmp_path, _nonphysical_entry(tmp_path), rows)
    built = sources.build(tmp_path)
    assert SUPPORT in _stale(built)
    assert "support" not in {use[2] for use in built.uses(sources.records())}


def test_nonphysical_support_requires_a_negative_column(tmp_path: Path):
    """A nonphysical projection naming a physical column is refused at load."""
    entry = _nonphysical_entry(tmp_path)
    entry["column"] = "FIRST"
    sources = _nonphysical_sources(tmp_path, entry)
    with pytest.raises(RegMetaError) as raised:
        sources.build(tmp_path)
    assert raised.value.code == "classification_curation_invalid"
    assert raised.value.message.startswith("curation/registers/scb/sample.toml")
    assert "negative-column" in raised.value.message
