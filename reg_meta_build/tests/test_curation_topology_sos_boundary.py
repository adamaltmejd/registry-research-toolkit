"""SOS register topology from delivered subset names at the build boundary.

The synthetic BU workbook (derived from the default SYN fixture by `sos_register`)
has no Deldatamängder sheet: its variable rows name their subsets directly. The
curation TOML names those subsets as variants; the cases build through the real
pipeline and assert on the built catalog and the issue ledger.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _curation_support_boundary_support import (
    prepare_sources,
    sos_head,
    sos_id,
    sos_register,
)

if TYPE_CHECKING:
    from pathlib import Path

TITLE = "Barn och unga"
CURATION = sos_head("bu", TITLE, ("A", "B")) + (
    f'[[variable]]\nnative_id = "{sos_id("bu", "variable", "COL")}"\nslug = "col"\n'
)


def _build(tmp_path: Path, rows):
    sources = prepare_sources(
        tmp_path,
        sos_registers=(sos_register("BU", TITLE, rows),),
        curation={"sos/bu.toml": CURATION},
    )
    return sources.build(tmp_path)


def _errors(built) -> list[tuple[str, str]]:
    return [(issue["code"], issue["detail"]) for issue in built.errors()]


def test_named_subsets_without_a_subset_sheet_keep_their_variants(tmp_path: Path):
    """Subset names on the variable rows alone form the curated variants and tables."""
    built = _build(
        tmp_path,
        (
            {"name": "COL", "deldatamangd": "A", "label": "Col", "data_type": "Datum"},
            {"name": "COL", "deldatamangd": "B", "label": "Col"},
        ),
    )
    assert not _errors(built)
    assert built.rows(
        "SELECT rv.slug, rv.name FROM register_variant rv "
        "JOIN register r USING (register_id) WHERE r.slug = 'bu' ORDER BY 1"
    ) == [("a", "A"), ("b", "B")]
    assert built.rows(
        "SELECT v.slug, rv.slug, s.delivery_column_name, s.data_type "
        "FROM variable v JOIN register r USING (register_id) "
        "JOIN variable_state s USING (variable_id) "
        "JOIN register_variant rv ON rv.register_variant_id = s.register_variant_id "
        "WHERE r.slug = 'bu' ORDER BY 2"
    ) == [("col", "a", "COL", "date"), ("col", "b", "COL", "text")]


def test_a_named_subset_no_longer_delivered_is_stale(tmp_path: Path):
    """A curated variant whose subset name is no longer delivered is a stale entry."""
    built = _build(tmp_path, ({"name": "COL", "deldatamangd": "B", "label": "Col"},))
    assert _errors(built) == [
        (
            "stale_curation_entry",
            "curation/registers/sos/bu.toml [[variant]] entry 1 binds no native "
            "family or parent",
        )
    ]
    assert built.rows(
        "SELECT rv.slug FROM register_variant rv "
        "JOIN register r USING (register_id) WHERE r.slug = 'bu'"
    ) == [("b",)]
