"""Checked delivery metadata and parallel column representations at the build boundary.

Every case prepares synthetic SCB Registerinformation rows (`_csv_fixtures.var_row`;
register TESTREG = native register 1, variant 1.10) and a curation TOML through
the real pipeline. Guarded expectations are authored from the prepared records the
way a curator reads them (public `capture_expectations`). Assertions read the built
catalog, the issue ledger and each source occurrence's ledger disposition.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import replace_registerinformation_cell, var_row
from _curation_support_boundary_support import (
    SCB_SAMPLE,
    SCB_VARIANT,
    prepare_sources,
    toml_inline,
)
from reg_meta_build.source_coordinates import native_variant_key
from reg_meta_build.source_curation import capture_expectations
from reg_meta_build.source_records import SourceFields

if TYPE_CHECKING:
    from pathlib import Path

METADATA = "curation/registers/scb/sample.toml#/representation.delivery_metadata"
PARTITIONED = (
    SCB_SAMPLE
    + SCB_VARIANT
    + '[[variable]]\nnative_id = "1.5.ku"\nslug = "ku"\n'
    + '[[variable]]\nnative_id = "1.5.agi"\nslug = "agi"\n'
    + '[[identity.partition]]\nvariable = "1.5"\n'
    + 'columns = { KU = "1.5.ku", AGI = "1.5.agi" }\ncolumns_ref = "fixture"\n'
)


def _income(column, cvid, edition, regver, *, year="2020", name="Income", unit=None):
    return var_row(
        colname=column,
        cvid=cvid,
        var_id=5,
        varname=name,
        year=year,
        versionname=edition,
        regver_id=regver,
        vardef="Supplied income quantity",
        unit=unit or "100-tal kronor",
    )


def _income_rows(preliminary_unit: str | None) -> list[str]:
    """KU names its 2020 final and its 2021 delivery differently; AGI is a sibling.

    With ``preliminary_unit``, a superseded 2020 preliminary KU edition is delivered.
    """
    rows = [
        _income("KU", 31, "2020, slutlig version", 11),
        _income("AGI", 32, "2020, slutlig version", 11),
        _income("KU", 33, "2021", 2021, year="2021", name="Taxable income"),
    ]
    if preliminary_unit is not None:
        rows.append(
            _income("KU", 30, "2020, preliminär version", 10, unit=preliminary_unit)
        )
    return rows


def _capture(records) -> list[dict]:
    return [
        e.model_dump(mode="json")
        for e in capture_expectations(
            tuple(records),
            fields=tuple(SourceFields.model_fields),
            parents=True,
            coding=True,
        )
    ]


def _delivery_metadata(records) -> str:
    """Name metadata for the KU owner: its catalog deliveries, every peer as support."""
    ku = [
        r
        for r in records
        if r.fields.column_name.value == "KU" and r.subject.native.edition_id != 10
    ]
    variant = native_variant_key(ku[0])
    assert variant is not None
    entry = {
        "variable": "1.5.ku",
        "fields": ["name"],
        "records": _capture(ku),
        "support": _capture(r for r in records if r not in ku),
        "columns": [
            {
                "variant_key": list(variant),
                "column": "KU",
                "valid_from": "2020-01-01",
                "valid_to": "2021-12-31",
                "expected_codings": [],
            }
        ],
        "evidence": "Exact supplied KU names; AGI remains separate",
        "noted": "2026-09-30",
    }
    return "[[representation.delivery_metadata]]\n" + "".join(
        f"{key} = {toml_inline(value)}\n" for key, value in entry.items()
    )


def _ku_states(built) -> list[tuple]:
    return built.rows(
        "SELECT s.delivery_column_name, s.valid_from, s.name FROM variable v "
        "JOIN variable_state s USING (variable_id) WHERE v.slug = 'ku' ORDER BY 2"
    )


@pytest.mark.parametrize(
    "preliminary", [False, True], ids=["final-only", "with-preliminary"]
)
def test_delivery_names_stay_on_the_partition_owner_states(
    tmp_path: Path, preliminary: bool
):
    """Checked name metadata keeps each KU delivery name instead of withholding KU.

    It is checked against the partition-owned, preliminary-superseded occurrences:
    a superseded preliminary edition is support, never a named delivery.
    """
    sources = prepare_sources(
        tmp_path,
        scb_rows=_income_rows("Olika valutor" if preliminary else None),
        curation={"scb/sample.toml": PARTITIONED},
    )
    records = sources.records()
    sources.append("scb/sample.toml", _delivery_metadata(records))
    built = sources.build(tmp_path)
    assert [(i["code"], i["severity"], i["subject"]) for i in built.issues()] == [
        ("delivery_text_projected", "warning", "scb/sample/ku")
    ]
    assert _ku_states(built) == [
        ("KU", "2020-01-01", "Income"),
        ("KU", "2021-01-01", "Taxable income"),
    ]
    preliminary_use = [("KU", "member:30", "support", "scb/sample/ku")]
    assert built.uses(records) == [
        ("AGI", "member:32", "catalog", "scb/sample/agi"),
        *(preliminary_use if preliminary else []),
        ("KU", "member:31", "catalog", "scb/sample/ku"),
        ("KU", "member:33", "catalog", "scb/sample/ku"),
    ]


def test_delivery_names_are_stale_when_a_guarded_support_record_changes(
    tmp_path: Path,
):
    """A changed preliminary literal invalidates the metadata; KU names then conflict."""
    authored = prepare_sources(
        tmp_path / "authored",
        scb_rows=_income_rows("Olika valutor"),
        curation={"scb/sample.toml": PARTITIONED},
    )
    sources = prepare_sources(
        tmp_path,
        scb_rows=_income_rows("Changed literal"),
        curation={
            "scb/sample.toml": PARTITIONED + _delivery_metadata(authored.records())
        },
    )
    built = sources.build(tmp_path)
    errors = [
        (i["code"], i["subject"]) for i in built.issues() if i["severity"] == "error"
    ]
    assert ("stale_curation_entry", "1.5.ku") in errors
    assert ("unresolved_variable_name", "scb/sample/ku") in errors
    assert any(
        i["detail"].startswith(f"{METADATA}/1:")
        for i in built.issues("stale_curation_entry")
    )
    assert _ku_states(built) == []


PARALLEL = (
    '[[variable]]\nnative_id = "1.1"\nslug = "income"\n'
    '[[identity.partition]]\nvariable = "1.1"\n'
    'columns = { First = "1.1", Second = "1.1" }\n'
    'columns_ref = "reviewed parallel source-native quantity"\n'
    '[[representation.parallel]]\nvariable = "1.1"\nvariant = "1.10"\n'
    'valid_from = "2022-01-01"\nvalid_to = "2022-12-31"\n'
    'evidence = "Reviewed original boundary"\nnoted = "2026-09-29"\n'
    'columns = [{column = "First", valid_from = "2020-01-01", '
    'valid_to = "2022-12-31", source_editions = ["2020-2022", "2021-2022"]}, '
    '{column = "Second", valid_from = "2022-01-01", '
    'valid_to = "2024-12-31", source_editions = ["2022-2024"]}]\n'
    'column_metadata = "per_column"\n'
)


def _pooled(column, cvid, edition, regver, *, year="2020", **cells) -> str:
    row = var_row(
        colname=column,
        cvid=cvid,
        var_id=1,
        varname="Income",
        year=year,
        versionname=edition,
        regver_id=regver,
    )
    for name, value in cells.items():
        row = replace_registerinformation_cell(row, name, value)
    return row


def _parallel_rows(first=None, nested=None) -> list[str]:
    """First is pooled 2020-2022 and again nested 2021-2022; Second is 2022-2024."""
    return [
        _pooled("First", 100, "2020-2022", 110, **(first or {})),
        _pooled("Second", 101, "2022-2024", 111),
        _pooled("First", 103, "2021-2022", 113, year="2021", **(nested or {})),
    ]


def test_parallel_columns_keep_every_nested_edition_original(tmp_path: Path):
    """A reviewed parallel window keeps the pooled and the nested First editions."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=_parallel_rows(),
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + PARALLEL},
    )
    built = sources.build(tmp_path)
    assert not built.issues()
    assert built.uses(sources.records()) == [
        ("First", "member:100", "catalog", "scb/sample/income"),
        ("First", "member:103", "catalog", "scb/sample/income"),
        ("Second", "member:101", "catalog", "scb/sample/income"),
    ]
    assert {
        column
        for (column,) in built.rows(
            "SELECT DISTINCT delivery_column_name FROM variable_state"
            " WHERE variable_id IN (SELECT variable_id FROM variable WHERE slug='income')"
        )
    } == {"First", "Second"}


@pytest.mark.parametrize(
    ("curation", "first", "nested"),
    [
        pytest.param(
            PARALLEL.replace('valid_from = "2020-01-01"', 'valid_from = "2019-01-01"'),
            None,
            None,
            id="window-gap",
        ),
        pytest.param(
            PARALLEL.replace('valid_from = "2020-01-01"', 'valid_from = "2021-01-01"'),
            None,
            None,
            id="original-outside-window",
        ),
        pytest.param(
            PARALLEL,
            {"VariabelOperationell_definition": "A known quantity operation"},
            {"VariabelOperationell_definition": "A contrary quantity operation"},
            id="nested-metadata-conflict",
        ),
        pytest.param(
            PARALLEL,
            {"VariabelReferenstid": "A known quantity operation"},
            {"VariabelReferenstid": "A contrary quantity operation"},
            id="nested-reference-period-conflict",
        ),
    ],
)
def test_parallel_columns_are_stale_unless_nested_editions_cover_exactly(
    tmp_path: Path, curation: str, first, nested
):
    """A parallel window must match its originals' coverage and agreeing metadata."""
    sources = prepare_sources(
        tmp_path,
        scb_rows=_parallel_rows(first, nested),
        curation={"scb/sample.toml": SCB_SAMPLE + SCB_VARIANT + curation},
    )
    built = sources.build(tmp_path)
    assert [
        i["detail"].split(":", 1)[0] for i in built.issues("stale_curation_entry")
    ] == ["curation/registers/scb/sample.toml#/representation.parallel/1"]
    assert ("unresolved_column_representation", "scb/sample/income") in [
        (i["code"], i["subject"]) for i in built.issues()
    ]
