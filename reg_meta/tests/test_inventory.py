"""Steward delivery-inventory contract (REFACTOR_SPEC.md §12 lane 1).

The fixture below is a synthetic inventory — no real steward holdings are
committed here — covering the four shapes §12 calls out: a mapped column, an
unresolved (zero-mapping) column, one column mapped to two register variants
(the combined Utrikeshandel shape), and a table whose edition is a finite
multi-period list. The rejection cases pin the fail-fast guards.
"""

from __future__ import annotations

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidError
from reg_meta.inventory import (
    DeliveryInventory,
    EditionRange,
    edition_bounds,
    load_inventory,
)

FIXTURE_INVENTORY = """
version = 1
steward = "swecov"

# A CSV delivery: the identifier is the exact delivered filename; the edition is
# curated explicitly even though this filename happens to carry its year.
[[table]]
id = "LISA_Individ_2019.csv"
edition = 2019

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"

[[table.column]]
name = "DispInk04"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/disponibel-inkomst"
representation = "DispInk04"

# Delivered but not yet mapped: stays in the coverage denominator.
[[table.column]]
name = "LopNr"

# One SQL table serving two register variants (the Utrikeshandel shape).
[[table]]
id = "dbo.Utrikeshandel"
edition = { from = 2015, to = 2020 }

[[table.column]]
name = "Varukod"
[[table.column.mapping]]
register_variant = "scb/utrikeshandel/import"
variable = "scb/utrikeshandel/varukod"
[[table.column.mapping]]
register_variant = "scb/utrikeshandel/export"
variable = "scb/utrikeshandel/varukod"

# An interrupted series: a finite list of edition segments.
[[table]]
id = "SCB_Foretag_2005-2010_2015.csv"
edition = [{ from = 2005, to = 2010 }, "2015"]

[[table.column]]
name = "PeOrgNr"
"""


def _write(tmp_path, text: str):
    path = tmp_path / "inventory.toml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def inventory(tmp_path) -> DeliveryInventory:
    return load_inventory(_write(tmp_path, FIXTURE_INVENTORY))


def test_fixture_parses_into_typed_models(inventory: DeliveryInventory) -> None:
    assert inventory.version == 1
    assert inventory.steward == "swecov"
    assert [table.id for table in inventory.tables] == [
        "LISA_Individ_2019.csv",
        "dbo.Utrikeshandel",
        "SCB_Foretag_2005-2010_2015.csv",
    ]

    lisa = inventory.tables[0]
    # The bare TOML year int canonicalizes to its period token.
    assert lisa.edition == "2019"
    # Physical column names stay literal and case-preserving.
    assert [column.name for column in lisa.columns] == ["Kon", "DispInk04", "LopNr"]

    mapped = lisa.columns[0].mappings[0]
    assert mapped.register_variant == "scb/lisa/individer-15plus"
    assert str(mapped.variable) == "scb/lisa/kon"
    assert mapped.representation is None
    assert lisa.columns[1].mappings[0].representation == "DispInk04"


def test_unresolved_column_carries_no_mappings(inventory: DeliveryInventory) -> None:
    assert inventory.tables[0].columns[2].mappings == ()


def test_one_column_maps_to_two_variants(inventory: DeliveryInventory) -> None:
    varukod = inventory.tables[1].columns[0]
    assert [mapping.register_variant for mapping in varukod.mappings] == [
        "scb/utrikeshandel/import",
        "scb/utrikeshandel/export",
    ]
    assert {str(mapping.variable) for mapping in varukod.mappings} == {
        "scb/utrikeshandel/varukod"
    }


def test_fixture_round_trips_through_the_models(inventory: DeliveryInventory) -> None:
    dumped = inventory.model_dump()
    # The dump speaks the authored TOML spelling (singular array-of-tables keys,
    # `from` not `from_`), so it re-validates without translation.
    assert set(dumped) == {"version", "steward", "table"}
    assert set(dumped["table"][1]["edition"]) == {"from", "to"}
    assert DeliveryInventory.model_validate(dumped) == inventory


def test_edition_bounds_expand_via_the_shared_period_grammar() -> None:
    assert edition_bounds("2019") == (("2019-01-01", "2019-12-31"),)
    assert edition_bounds("2019-Q3") == (("2019-07-01", "2019-09-30"),)
    assert edition_bounds((EditionRange(**{"from": "2005", "to": "2010"}), "2015")) == (
        ("2005-01-01", "2010-12-31"),
        ("2015-01-01", "2015-12-31"),
    )


@pytest.mark.parametrize(
    ("edition", "message"),
    [
        ('edition = "_default"', "never '_default'"),
        ('edition = "all"', "not a period token"),
        ("edition = 2019.5", "Input should be"),
        ("edition = { from = 2020, to = 2015 }", "'from' is after 'to'"),
        ("edition = []", "must not be empty"),
        ('edition = ["2015", "2010"]', "sorted ascending and non-overlapping"),
        (
            'edition = [{ from = 2005, to = 2012 }, "2010"]',
            "sorted ascending and non-overlapping",
        ),
    ],
)
def test_rejects_non_finite_or_malformed_editions(
    tmp_path, edition: str, message: str
) -> None:
    text = f"""
version = 1
steward = "swecov"

[[table]]
id = "SoS_Patientregister.csv"
{edition}

[[table.column]]
name = "Diagnos"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    assert excinfo.value.exit_code == EXIT_CONFIG
    # The error names the offending table, not an array index.
    assert "table['SoS_Patientregister.csv'].edition" in excinfo.value.message
    assert message in excinfo.value.message


def test_rejects_table_without_an_edition(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "Holdings_Extract.csv"

[[table.column]]
name = "Diagnos"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    assert "table['Holdings_Extract.csv'].edition: Field required" in (
        excinfo.value.message
    )


@pytest.mark.parametrize(
    ("mapping", "message"),
    [
        ('variable = "scb/lisa"', "3-segment binding FQID"),
        ('variable = "scb/fek/kon"', "does not belong to register_variant"),
        ('variable = "scb/lisa/kon"\nvariabel = "typo"', "Extra inputs"),
    ],
)
def test_rejects_malformed_mappings(tmp_path, mapping: str, message: str) -> None:
    text = f"""
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2019.csv"
edition = 2019

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
{mapping}
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    # The error names the offending table AND column.
    assert "table['LISA_Individ_2019.csv'].column['Kon'].mapping[0]" in (
        excinfo.value.message
    )
    assert message in excinfo.value.message


def test_rejects_duplicate_table_identifier(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ.csv"
edition = 2019
[[table.column]]
name = "Kon"

[[table]]
id = "LISA_Individ.csv"
edition = 2020
[[table.column]]
name = "Kon"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert "duplicate table 'LISA_Individ.csv'" in excinfo.value.message


def test_rejects_duplicate_physical_column(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ.csv"
edition = 2019
[[table.column]]
name = "Kon"
[[table.column]]
name = "Kon"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert "duplicate physical column 'Kon'" in excinfo.value.message


def test_rejects_an_inventory_with_no_tables(tmp_path) -> None:
    """The inventory is the authoritative holdings statement: an empty one is a
    curation error, never a steward that delivers nothing."""
    text = """
version = 1
steward = "swecov"
table = []
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    assert excinfo.value.exit_code == EXIT_CONFIG
    assert "table: Value error, inventory declares no tables" in excinfo.value.message


def test_rejects_a_table_with_no_columns(tmp_path) -> None:
    text = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ.csv"
edition = 2019
column = []
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert excinfo.value.code == "inventory_invalid"
    # The error names the offending table, not an array index.
    assert (
        "table['LISA_Individ.csv'].column: Value error, table declares no columns"
        in (excinfo.value.message)
    )


def test_rejects_unknown_contract_version(tmp_path) -> None:
    text = """
version = 2
steward = "swecov"

[[table]]
id = "LISA_Individ.csv"
edition = 2019
[[table.column]]
name = "Kon"
"""
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, text))
    assert "version:" in excinfo.value.message


def test_rejects_unreadable_toml(tmp_path) -> None:
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(_write(tmp_path, "version = 1\nsteward = swecov\n"))
    assert excinfo.value.code == "inventory_toml_unreadable"
    assert excinfo.value.exit_code == EXIT_CONFIG


def test_rejects_a_non_utf8_inventory(tmp_path) -> None:
    """TOML is UTF-8 by definition; a mis-encoded file is unreadable input on
    the documented path, not an uncaught `UnicodeDecodeError`."""
    path = tmp_path / "inventory.toml"
    path.write_bytes(b'version = 1\nsteward = "swecov"\n# \xff\xfe not utf-8\n')
    with pytest.raises(RegMetaError) as excinfo:
        load_inventory(path)
    assert excinfo.value.code == "inventory_toml_unreadable"
    assert excinfo.value.exit_code == EXIT_CONFIG


def test_edition_bounds_rejects_a_non_period_token() -> None:
    with pytest.raises(FqidError):
        edition_bounds("2019-2020")


def test_accepts_the_default_variant_coordinate(tmp_path) -> None:
    """A single-table register rides the synthesized `_default` variant slug;
    only the EDITION forbids `_default` (§12)."""
    text = """
version = 1
steward = "swecov"

[[table]]
id = "SmiNet_2021.csv"
edition = 2021
[[table.column]]
name = "Diagnosdatum"
[[table.column.mapping]]
register_variant = "fohm/sminet/_default"
variable = "fohm/sminet/diagnosdatum"
"""
    inventory = load_inventory(_write(tmp_path, text))
    mapping = inventory.tables[0].columns[0].mappings[0]
    assert mapping.register_variant == "fohm/sminet/_default"
