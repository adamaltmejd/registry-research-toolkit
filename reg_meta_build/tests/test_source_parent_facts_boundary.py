"""Parent-fact occurrences at the prepared-input reader boundary.

The source is a synthetic SCB Registerinformation export (rows from `var_row`)
prepared through the real catalog-input pipeline and read back with
`open_prepared_catalog_sources`. It is modelled on the deleted
`test_source_parent_facts.py` case: one row delivered twice, and a third row that
differs only in a register-level parent fact (`Registersyfte`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _csv_fixtures import (
    REGISTERINFORMATION_HEADER,
    var_row,
    write_input_bundle,
    write_scb_input,
)
from _prepared_fixtures import accept_prepared
from reg_meta_build.prepared_catalog import (
    open_prepared_catalog_sources,
    prepare_catalog_sources,
)

if TYPE_CHECKING:
    from pathlib import Path

ROW = var_row(cvid=1001, var_id=101, colname="VALUE")
_CELLS = ROW.split("|")
_CELLS[REGISTERINFORMATION_HEADER.split("|").index("Registersyfte")] = "Another purpose"
CONFLICT = "|".join(_CELLS)


def test_duplicate_rows_and_parent_conflicts_stay_distinct_located_occurrences(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=[ROW, ROW, CONFLICT],
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
        ],
        include=("registerinformation", "unika"),
    )
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    opened = open_prepared_catalog_sources(
        prepared,
        expected_sha256=manifest.sha256,
        input_commit=accept_prepared(prepared),
    )
    first, duplicate, conflict = (
        record
        for record in opened.records.records
        if record.source == "scb-registerinformation"
    )
    # The repeated row is the same semantic record delivered twice; the conflict
    # is a different record although its own variable fields are identical.
    assert first.record_id == duplicate.record_id != conflict.record_id
    assert first.fields == conflict.fields
    assert [
        (
            record.parent_facts[0].fields.model_dump()["purpose"]["value"],
            record.parent_field_locators(0, "purpose")[0].physical_cells,
        )
        for record in (first, duplicate, conflict)
    ] == [
        ("Testning", ("Registerinformation.csv:row:2:Registersyfte",)),
        ("Testning", ("Registerinformation.csv:row:3:Registersyfte",)),
        ("Another purpose", ("Registerinformation.csv:row:4:Registersyfte",)),
    ]
