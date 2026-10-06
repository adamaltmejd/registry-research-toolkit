"""SOS inline `Värdemängd` enumerations as source records.

`cases/sos-inline-value-set.json` lists delivered `Värdemängd` cells and the
inline value set the source-record reader states for each: the ordered
`(code, label)` members, and whether the descriptor marks the list unresolved
(`null` means no inline descriptor at all, i.e. the cell stays free text).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import openpyxl
import pytest
from _sos_fixtures import (
    BU_SPEC_LINED,
    BU_SPEC_ONE_LINE,
    BU_SPEC_WRAPPED,
    source_revision,
    write_source_workbook,
)
from reg_meta_build.sources.sos import parse_register_file
from reg_meta_build.sources.sos_records import clean_sos_source

INLINE_CASES = json.loads(
    (Path(__file__).parent / "cases/sos-inline-value-set.json").read_text(
        encoding="utf-8"
    )
)


def test_bu_spec_fixture_cells_match_the_retained_source_evidence() -> None:
    # These digests come from the retained BU source evidence, NOT from the fixture
    # constants: the real workbook is gitignored, so this is what keeps the cells
    # below byte-exact. A failure means a fixture drifted from the original cell —
    # restore the cell rather than recomputing the digest.
    assert [
        hashlib.sha256(cell.encode()).hexdigest()
        for cell in (BU_SPEC_LINED, BU_SPEC_WRAPPED, BU_SPEC_ONE_LINE)
    ] == [
        "809e5e939201350001edb0483b236a5c896d7dec6cab6d0f75c48d9e314b91bf",
        "1b9ab9b4b525bb19b01d62fc47d83b04d177242d7d5b2ec8d8626cef652b0712",
        "0a62d6be738d2763e128c5aac35adba216a489069a2ed7ff0d1ec54e2b1d3b07",
    ]


@pytest.mark.parametrize("case", INLINE_CASES, ids=lambda case: case["id"])
def test_inline_value_set_cell_states_members_or_unresolved(
    tmp_path: Path, case: dict
) -> None:
    path = tmp_path / "Metadata Test.xlsx"
    write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    workbook["Metadata - Variabelnivå"]["E2"] = case["cell"]
    workbook.save(path)

    cleaned = clean_sos_source(parse_register_file(path), source_revision(path))
    record = next(
        record
        for record in cleaned.records
        if record.locators[0].physical_record == "row:2"
        and record.subject.member.name == "HDIA"
    )
    descriptor_key = f"inline:{record.record_id}"
    assert [
        list(cleaned.values[association.value_key].normalized_content)
        for association in cleaned.associations
        if association.descriptor_key == descriptor_key
    ] == case["members"]
    descriptor = cleaned.descriptors.get(descriptor_key)
    unresolved = None if descriptor is None else descriptor.unresolved_members
    assert unresolved == case["unresolved"]
